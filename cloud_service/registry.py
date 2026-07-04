"""Thread-safe in-memory robot registry with heartbeat-staleness detection.

A background thread periodically marks robots `offline` when their heartbeat
goes silent beyond HEARTBEAT_TIMEOUT_S, and cleans up any spawned Player.
"""

import threading
import time
from typing import Optional

from cloud_service.logger import get_logger
from cloud_service.protocol import HEARTBEAT_TIMEOUT_S

logger = get_logger("registry")

# Registry entry structure:
# {
#   "status": "online" | "offline",
#   "last_heartbeat": float (time.monotonic),
#   "address": str (IP reported by robot),
#   "pub_port": int (assigned to robot),
#   "push_port": int (assigned for PULL socket, used by Cloud to push mesh info),
#   "player_pub_port": Optional[int],
#   "user_pub_port": Optional[int],
#   "user_address": Optional[str],
#   "pending_mesh_info": Optional[dict],  # legacy, kept for heartbeat fallback
# }

RobotEntry = dict


class Registry:
    """Holds all known robots. Thread-safe via a lock."""

    def __init__(self) -> None:
        self._robots: dict[str, RobotEntry] = {}
        self._lock = threading.Lock()
        self._on_offline: Optional[callable] = None  # callback(robot_id)
        self._stop_event = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="registry-monitor"
        )
        self._monitor_thread.start()

    def set_on_offline(self, callback: callable) -> None:
        """Register a callback invoked when a robot is marked offline."""
        self._on_offline = callback

    # ── Public API ──────────────────────────────────────────────────────

    def register(self, robot_id: str, address: str, pub_port: int, push_port: int) -> Optional[str]:
        """Register a new robot.

        Returns None on success, or an error string if the robot_id is
        already registered and still online.  A robot that was marked
        offline may re-register freely.
        """
        with self._lock:
            existing = self._robots.get(robot_id)
            if existing and existing["status"] == "online":
                return f"Robot '{robot_id}' is already registered and online"
            self._robots[robot_id] = {
                "status": "online",
                "last_heartbeat": time.monotonic(),
                "address": address,
                "pub_port": pub_port,
                "push_port": push_port,
                "player_pub_port": None,
                "user_pub_port": None,
                "user_address": None,
                "pending_mesh_info": None,
            }
            logger.info("Registered robot '%s' on %s:%s (push %s)", robot_id, address, pub_port, push_port)
            return None

    def heartbeat(self, robot_id: str) -> Optional[dict]:
        """Record a heartbeat. Returns any pending mesh_info for the robot."""
        with self._lock:
            entry = self._robots.get(robot_id)
            if entry is None:
                return None
            entry["status"] = "online"
            entry["last_heartbeat"] = time.monotonic()
            info = entry.get("pending_mesh_info")
            entry["pending_mesh_info"] = None
            return info

    def get_online_robots(self) -> list[dict]:
        """Return summary of all online robots (for LIST_ROBOTS)."""
        with self._lock:
            return [
                {"robot_id": rid, "address": e["address"], "pub_port": e["pub_port"]}
                for rid, e in self._robots.items()
                if e["status"] == "online"
            ]

    def get_robot(self, robot_id: str) -> Optional[RobotEntry]:
        with self._lock:
            return self._robots.get(robot_id)

    def set_mesh_info(self, robot_id: str, info: dict) -> None:
        """Store mesh info to be delivered on the robot's next heartbeat."""
        with self._lock:
            entry = self._robots.get(robot_id)
            if entry:
                entry["pending_mesh_info"] = info

    def set_player_port(self, robot_id: str, port: int) -> None:
        with self._lock:
            entry = self._robots.get(robot_id)
            if entry:
                entry["player_pub_port"] = port

    def set_user_info(self, robot_id: str, pub_port: int, address: str) -> None:
        with self._lock:
            entry = self._robots.get(robot_id)
            if entry:
                entry["user_pub_port"] = pub_port
                entry["user_address"] = address

    def mark_offline(self, robot_id: str) -> None:
        """Force-mark a robot offline (and deliver the callback)."""
        with self._lock:
            entry = self._robots.get(robot_id)
            if entry is None:
                return
            entry["status"] = "offline"
            entry["player_pub_port"] = None
            entry["user_pub_port"] = None
            entry["user_address"] = None
            entry["pending_mesh_info"] = None
            logger.info("Marked robot '%s' offline (heartbeat timeout)", robot_id)
        if self._on_offline:
            self._on_offline(robot_id)

    def shutdown(self) -> None:
        self._stop_event.set()
        self._monitor_thread.join(timeout=3)

    # ── Internal ────────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        """Periodically sweep for stale entries."""
        while not self._stop_event.is_set():
            time.sleep(1)
            now = time.monotonic()
            stale_ids: list[str] = []
            with self._lock:
                for rid, entry in self._robots.items():
                    if entry["status"] == "online":
                        age = now - entry["last_heartbeat"]
                        if age > HEARTBEAT_TIMEOUT_S:
                            stale_ids.append(rid)
            for rid in stale_ids:
                self.mark_offline(rid)

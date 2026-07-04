"""Manages Player subprocess lifecycle per connected robot.

Each Player is spawned as a subprocess running:
    python -m cloud_service.player --robot-id <id> --pub-port <port> \\
        --robot-addr <addr> --user-addr <addr>

Tracks processes by robot_id so they can be killed when a robot goes
offline or the Cloud shuts down.
"""

import subprocess
import sys
from typing import Optional

from cloud_service.logger import get_logger

logger = get_logger("player_manager")


class PlayerManager:
    """Spawns and kills Player subprocesses."""

    def __init__(self) -> None:
        self._processes: dict[str, subprocess.Popen] = {}

    def spawn(self, robot_id: str, pub_port: int,
              robot_addr: str, user_addr: str) -> Optional[str]:
        """Launch a Player subprocess for *robot_id*.

        Returns None on success, or an error message on failure.
        """
        if robot_id in self._processes:
            proc = self._processes[robot_id]
            if proc.poll() is None:
                logger.warning(
                    "Player for '%s' already running (pid %s)", robot_id, proc.pid
                )
                return None  # already running
            # Process died unexpectedly — remove stale entry
            logger.info("Removing stale Player entry for '%s'", robot_id)
            del self._processes[robot_id]

        args = [
            sys.executable, "-m", "cloud_service.player",
            "--robot-id", robot_id,
            "--pub-port", str(pub_port),
            "--robot-addr", robot_addr,
            "--user-addr", user_addr,
        ]
        try:
            proc = subprocess.Popen(
                args,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            self._processes[robot_id] = proc
            logger.info(
                "Spawned Player for '%s' on port %s (pid %s)",
                robot_id, pub_port, proc.pid,
            )
            return None
        except OSError as exc:
            msg = f"Failed to spawn Player for '{robot_id}': {exc}"
            logger.error(msg)
            return msg

    def kill(self, robot_id: str) -> None:
        """Terminate the Player for *robot_id* if running."""
        proc = self._processes.pop(robot_id, None)
        if proc is None:
            return
        if proc.poll() is None:
            logger.info("Killing Player for '%s' (pid %s)", robot_id, proc.pid)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Player for '%s' didn't terminate, killing", robot_id)
                proc.kill()
                proc.wait(timeout=3)

    def kill_all(self) -> None:
        """Terminate every tracked Player (used during Cloud shutdown)."""
        for robot_id in list(self._processes.keys()):
            self.kill(robot_id)

    def is_running(self, robot_id: str) -> bool:
        proc = self._processes.get(robot_id)
        return proc is not None and proc.poll() is None

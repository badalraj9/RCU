"""Shared protocol constants, topic helpers, and message schemas.

This module is duplicated identically in cloud_service/, robot/, and user/
to avoid a shared/ folder while respecting the required directory structure.
"""

import json
import os

# ── Control plane ──────────────────────────────────────────────────────────
CONTROL_PORT = int(os.environ.get("RCU_CONTROL_PORT", "5555"))
CONTROL_ADDR = f"tcp://127.0.0.1:{CONTROL_PORT}"

# ── Port pool ─────────────────────────────────────────────────────────────
BASE_PUB_PORT = int(os.environ.get("RCU_BASE_PUB_PORT", "6000"))

# ── Timeouts ──────────────────────────────────────────────────────────────
HEARTBEAT_TIMEOUT_S = int(os.environ.get("RCU_HEARTBEAT_TIMEOUT", "5"))
HEARTBEAT_INTERVAL_S = int(os.environ.get("RCU_HEARTBEAT_INTERVAL", "2"))
SENSOR_INTERVAL_S = int(os.environ.get("RCU_SENSOR_INTERVAL", "1"))

# ── Auth (bonus: shared-secret check on control-plane requests) ───────────
# Must be set via RCU_SECRET env var. The process will not start without it.
RCU_SECRET_ENV = "RCU_SECRET"
SHARED_SECRET = os.environ.get(RCU_SECRET_ENV)
if not SHARED_SECRET:
    raise RuntimeError(
        "RCU_SECRET environment variable not set. "
        "Run: export RCU_SECRET=<your-secret> before starting."
    )

# ── Control-plane message types ──────────────────────────────────────────
MSG_REGISTER = "REGISTER"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_LIST_ROBOTS = "LIST_ROBOTS"
MSG_CONNECT_ROBOT = "CONNECT_ROBOT"

STATUS_OK = "ok"
STATUS_ERROR = "error"


# ── Data-plane topic helpers ──────────────────────────────────────────────

def topic_sensor(robot_id: str) -> str:
    return f"robot/{robot_id}/sensor"


def topic_command(robot_id: str) -> str:
    return f"robot/{robot_id}/command"


def topic_status(robot_id: str) -> str:
    return f"robot/{robot_id}/status"


def topic_processed(robot_id: str) -> str:
    return f"robot/{robot_id}/processed"


# ── Message helpers ───────────────────────────────────────────────────────

def encode_msg(topic: str, payload: dict) -> list[bytes]:
    return [topic.encode("utf-8"), json.dumps(payload).encode("utf-8")]


def decode_topic(frame: bytes) -> str:
    return frame.decode("utf-8")


def decode_payload(frame: bytes) -> dict:
    return json.loads(frame.decode("utf-8"))

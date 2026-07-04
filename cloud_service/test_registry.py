"""Unit tests for cloud_service.registry — thread-safe registry with heartbeat timeout.

Run with: python -m pytest cloud_service/test_registry.py
"""

import threading
import time

from cloud_service.registry import Registry


_PUSH_PORT = 9000


def _pp() -> int:
    global _PUSH_PORT
    _PUSH_PORT += 1
    return _PUSH_PORT


def test_register_new_robot() -> None:
    reg = Registry()
    err = reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    assert err is None
    entry = reg.get_robot("robot-1")
    assert entry is not None
    assert entry["status"] == "online"
    assert entry["pub_port"] == 6000


def test_register_duplicate_rejected() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    err = reg.register("robot-1", "tcp://127.0.0.1", 6001, _pp())
    assert err is not None
    assert "already registered" in err


def test_reconnect_after_offline() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    reg.mark_offline("robot-1")
    err = reg.register("robot-1", "tcp://127.0.0.1", 6001, _pp())
    assert err is None
    entry = reg.get_robot("robot-1")
    assert entry is not None
    assert entry["status"] == "online"
    assert entry["pub_port"] == 6001


def test_heartbeat_updates_timestamp() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    old_ts = reg.get_robot("robot-1")["last_heartbeat"]
    time.sleep(0.2)
    reg.heartbeat("robot-1")
    new_ts = reg.get_robot("robot-1")["last_heartbeat"]
    assert new_ts > old_ts


def test_heartbeat_returns_mesh_info() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    info = {"player_pub_addr": "tcp://127.0.0.1:7000"}
    reg.set_mesh_info("robot-1", info)
    result = reg.heartbeat("robot-1")
    assert result == info
    result2 = reg.heartbeat("robot-1")
    assert result2 is None


def test_get_online_robots() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    reg.register("robot-2", "tcp://127.0.0.1", 6001, _pp())
    online = reg.get_online_robots()
    assert len(online) == 2
    reg.mark_offline("robot-1")
    online = reg.get_online_robots()
    assert len(online) == 1
    assert online[0]["robot_id"] == "robot-2"


def test_offline_callback() -> None:
    called = []

    def callback(robot_id: str) -> None:
        called.append(robot_id)

    reg = Registry()
    reg.set_on_offline(callback)
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
    reg.mark_offline("robot-1")
    assert called == ["robot-1"]


def test_heartbeat_for_nonexistent_robot() -> None:
    reg = Registry()
    result = reg.heartbeat("nonexistent")
    assert result is None


def test_concurrent_register_and_heartbeat() -> None:
    reg = Registry()
    reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())

    stop = threading.Event()
    errors: list[Exception] = []

    def register_loop() -> None:
        while not stop.is_set():
            try:
                reg.register("robot-1", "tcp://127.0.0.1", 6000, _pp())
            except Exception as e:
                errors.append(e)

    def heartbeat_loop() -> None:
        while not stop.is_set():
            try:
                reg.heartbeat("robot-1")
            except Exception as e:
                errors.append(e)

    t1 = threading.Thread(target=register_loop, daemon=True)
    t2 = threading.Thread(target=heartbeat_loop, daemon=True)
    t1.start()
    t2.start()

    time.sleep(1.0)
    stop.set()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert not errors, f"Concurrent access raised exceptions: {errors}"
    entry = reg.get_robot("robot-1")
    assert entry is not None
    assert entry["status"] == "online"

"""Integration test — spins up Cloud, a fake Robot, and a fake User in-process.

Verifies that a sensor message published by the Robot is received by
a subscribed User within a timeout.

Run with: python -m pytest test_integration.py -v -s
"""

import json
import os
import threading
import time

import zmq

# Must set RCU_SECRET before importing protocol (which raises if unset).
os.environ["RCU_SECRET"] = "rcu-test-secret"

from cloud_service.protocol import (  # noqa: E402
    SHARED_SECRET,
    topic_sensor,
)
from cloud_service.registry import Registry


def _run_cloud(registry: Registry, stop_event: threading.Event,
               control_addr: str,
               base_port: int = 15000) -> None:  # high default to avoid TIME_WAIT clashes
    """Run Cloud REP loop on *control_addr* in a background thread."""
    ctx = zmq.Context()
    rep_sock = ctx.socket(zmq.REP)
    rep_sock.bind(control_addr)

    next_port = base_port

    while not stop_event.is_set():
        if rep_sock.poll(timeout=200) == 0:
            continue
        try:
            msg = rep_sock.recv_json()
        except zmq.ZMQError:
            break

        msg_type = msg.get("type", "")
        robot_id = msg.get("robot_id", "")

        if msg_type == "REGISTER":
            token = msg.get("token", "")
            if token != SHARED_SECRET:
                rep_sock.send_json({"status": "error", "message": "Invalid token"})
                continue
            port = next_port
            next_port += 1
            push_port = next_port
            next_port += 1
            err = registry.register(robot_id, msg.get("address", ""), port, push_port)
            if err:
                rep_sock.send_json({"status": "error", "message": err})
            else:
                rep_sock.send_json({"status": "ok", "pub_port": port, "push_port": push_port})

        elif msg_type == "HEARTBEAT":
            info = registry.heartbeat(robot_id)
            rep_sock.send_json({"status": "ok", "mesh_info": info})

        elif msg_type == "LIST_ROBOTS":
            robots = registry.get_online_robots()
            rep_sock.send_json({"status": "ok", "robots": robots})

        elif msg_type == "CONNECT_ROBOT":
            token = msg.get("token", "")
            if token != SHARED_SECRET:
                rep_sock.send_json({"status": "error", "message": "Invalid token"})
                continue
            entry = registry.get_robot(robot_id)
            if entry is None or entry["status"] != "online":
                rep_sock.send_json({
                    "status": "error",
                    "message": f"Robot '{robot_id}' not online",
                })
                continue
            user_port = next_port
            next_port += 1
            player_port = next_port
            next_port += 1

            robot_addr = f"tcp://127.0.0.1:{entry['pub_port']}"
            player_addr = f"tcp://127.0.0.1:{player_port}"

            registry.set_player_port(robot_id, player_port)
            registry.set_user_info(robot_id, user_port, f"tcp://127.0.0.1:{user_port}")
            registry.set_mesh_info(robot_id, {
                "robot_pub_addr": robot_addr,
                "player_pub_addr": player_addr,
                "user_pub_addr": f"tcp://127.0.0.1:{user_port}",
                "user_pub_port": user_port,
            })

            rep_sock.send_json({
                "status": "ok",
                "robot_pub_addr": robot_addr,
                "player_pub_addr": player_addr,
                "user_pub_port": user_port,
            })

        else:
            rep_sock.send_json({"status": "error", "message": f"Unknown type: {msg_type}"})

    rep_sock.close(linger=0)
    ctx.term()


class TestIntegration:
    """Integration tests using isolated control ports."""

    _next_port = 15555  # avoid clashing with real Cloud on 5555

    @classmethod
    def _next_control_port(cls) -> int:
        port = cls._next_port
        cls._next_port += 10
        return port

    def test_sensor_message_delivery(self) -> None:
        """Robot publishes sensor → User receives it via direct SUB."""
        control_port = self._next_control_port()
        control_addr = f"tcp://127.0.0.1:{control_port}"

        stop_event = threading.Event()
        registry = Registry()

        cloud_thread = threading.Thread(
            target=_run_cloud, args=(registry, stop_event, control_addr),
            daemon=True,
        )
        cloud_thread.start()
        time.sleep(0.3)

        ctx = zmq.Context()

        # Robot: register, get port, bind PUB
        req = ctx.socket(zmq.REQ)
        req.connect(control_addr)
        req.send_json({
            "type": "REGISTER", "robot_id": "test-robot",
            "address": "tcp://127.0.0.1", "token": SHARED_SECRET,
        })
        reply = req.recv_json()
        assert reply["status"] == "ok", f"Register failed: {reply}"
        robot_port = reply["pub_port"]
        req.close()

        robot_pub = ctx.socket(zmq.PUB)
        robot_pub.bind(f"tcp://127.0.0.1:{robot_port}")

        # User: CONNECT_ROBOT, bind PUB, open SUB to Robot
        req2 = ctx.socket(zmq.REQ)
        req2.connect(control_addr)
        req2.send_json({
            "type": "CONNECT_ROBOT", "robot_id": "test-robot",
            "token": SHARED_SECRET,
        })
        reply2 = req2.recv_json()
        assert reply2["status"] == "ok", f"Connect failed: {reply2}"
        req2.close()

        robot_pub_addr = reply2["robot_pub_addr"]
        user_port = reply2["user_pub_port"]

        user_pub = ctx.socket(zmq.PUB)
        user_pub.bind(f"tcp://127.0.0.1:{user_port}")

        user_sub = ctx.socket(zmq.SUB)
        user_sub.setsockopt_string(zmq.SUBSCRIBE, topic_sensor("test-robot"))
        user_sub.connect(robot_pub_addr)

        time.sleep(0.3)  # let ZMQ connections establish

        # Robot publishes sensor data
        sensor_payload = {"state": 42.0, "timestamp": time.time()}
        topic = topic_sensor("test-robot")
        msg = [topic.encode("utf-8"), json.dumps(sensor_payload).encode("utf-8")]
        robot_pub.send_multipart(msg)

        # User receives
        received = None
        deadline = time.time() + 5
        while time.time() < deadline:
            if user_sub.poll(timeout=300):
                topic_bytes, payload_bytes = user_sub.recv_multipart()
                received_topic = topic_bytes.decode("utf-8")
                received_payload = json.loads(payload_bytes.decode("utf-8"))
                received = (received_topic, received_payload)
                break
            robot_pub.send_multipart(msg)  # retry for late joiner
            time.sleep(0.1)

        robot_pub.close(linger=0)
        user_pub.close(linger=0)
        user_sub.close(linger=0)
        ctx.term()
        stop_event.set()

        assert received is not None, "No sensor message received by User"
        topic, payload = received
        assert topic == topic_sensor("test-robot")
        assert payload["state"] == 42.0

    def test_robot_register_reject_duplicate(self) -> None:
        """Verify duplicate REGISTER while online is rejected."""
        control_port = self._next_control_port()
        control_addr = f"tcp://127.0.0.1:{control_port}"

        stop_event = threading.Event()
        registry = Registry()

        cloud_thread = threading.Thread(
            target=_run_cloud, args=(registry, stop_event, control_addr),
            daemon=True,
        )
        cloud_thread.start()
        time.sleep(0.3)

        ctx = zmq.Context()

        def register(rid: str) -> dict:
            req = ctx.socket(zmq.REQ)
            req.connect(control_addr)
            req.send_json({
                "type": "REGISTER", "robot_id": rid,
                "address": "tcp://127.0.0.1", "token": SHARED_SECRET,
            })
            reply = req.recv_json()
            req.close()
            return reply

        reply1 = register("dup-robot")
        assert reply1["status"] == "ok"

        reply2 = register("dup-robot")
        assert reply2["status"] == "error"
        assert "already registered" in reply2.get("message", "")

        ctx.term()
        stop_event.set()

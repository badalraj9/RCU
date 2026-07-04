"""Robot client — registers with Cloud, forms pub/sub mesh, publishes sensor data.

Lifecycle:
1. Connect to Cloud → REGISTER → get pub_port + push_port → bind PUB + PULL.
2. Start heartbeat + sensor threads.
3. Poll PULL socket for mesh info pushed by Cloud when a User connects.
4. On mesh_info → open SUB socket to User → publish "ready" status.
5. Poll SUB for commands, publish sensor data and status.
"""

import signal
import threading
import time
from typing import Optional

import zmq

from robot.logger import get_logger
from robot.protocol import (
    CONTROL_ADDR,
    HEARTBEAT_INTERVAL_S,
    SENSOR_INTERVAL_S,
    SHARED_SECRET,
    decode_payload,
    decode_topic,
    encode_msg,
    topic_command,
    topic_sensor,
    topic_status,
)
from robot.robot_sdk import RobotSDK

logger = get_logger("robot_client")


class RobotClient:
    """Connects a single robot to the Cloud and the pub/sub mesh."""

    def __init__(self, robot_id: str) -> None:
        self.robot_id = robot_id
        self.sdk = RobotSDK()

        self._ctx = zmq.Context()
        self._pub_sock: Optional[zmq.Socket] = None
        self._pull_sock: Optional[zmq.Socket] = None
        self._sub_user: Optional[zmq.Socket] = None

        self._pub_port: Optional[int] = None
        self._push_port: Optional[int] = None
        self._mesh_info: Optional[dict] = None
        self._subs_open = False
        self._stop_event = threading.Event()

    def _cloud_request(self, msg: dict) -> dict:
        sock = self._ctx.socket(zmq.REQ)
        sock.connect(CONTROL_ADDR)
        sock.send_json(msg)
        reply = sock.recv_json()
        sock.close(linger=0)
        return reply

    def _register(self) -> bool:
        reply = self._cloud_request({
            "type": "REGISTER",
            "robot_id": self.robot_id,
            "address": "tcp://127.0.0.1",
            "token": SHARED_SECRET,
        })
        if reply.get("status") != "ok":
            logger.error("Registration failed: %s", reply.get("message"))
            return False
        self._pub_port = reply["pub_port"]
        self._push_port = reply["push_port"]
        logger.info(
            "Registered with Cloud, assigned PUB port %s, PUSH port %s",
            self._pub_port, self._push_port,
        )
        return True

    def _send_heartbeat(self) -> None:
        reply = self._cloud_request({
            "type": "HEARTBEAT",
            "robot_id": self.robot_id,
        })
        if reply.get("status") != "ok":
            logger.warning("Heartbeat failed: %s", reply.get("message"))

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            self._send_heartbeat()
            time.sleep(HEARTBEAT_INTERVAL_S)

    def _bind_pub(self) -> None:
        addr = f"tcp://127.0.0.1:{self._pub_port}"
        self._pub_sock = self._ctx.socket(zmq.PUB)
        self._pub_sock.bind(addr)
        logger.info("Robot PUB bound to %s", addr)

    def _bind_pull(self) -> None:
        addr = f"tcp://127.0.0.1:{self._push_port}"
        self._pull_sock = self._ctx.socket(zmq.PULL)
        self._pull_sock.bind(addr)
        logger.info("Robot PULL bound to %s (for mesh info push)", addr)

    def _open_sub_sockets(self) -> None:
        info = self._mesh_info
        if info is None:
            return

        user_addr = info["user_pub_addr"]
        self._sub_user = self._ctx.socket(zmq.SUB)
        self._sub_user.connect(user_addr)
        self._sub_user.setsockopt_string(
            zmq.SUBSCRIBE, topic_command(self.robot_id)
        )
        logger.info("Robot SUB -> User at %s", user_addr)
        self._subs_open = True

        # Retry-publish "ready" status: ZMQ PUB has no ack and the first
        # publish is often lost when the receiver's SUB connect hasn't
        # completed asynchronously.  We send up to 10 times (100ms apart)
        # to give the subscriber window a chance to open.  The receiver
        # (User CLI) also waits with a timeout, so this is belt-and-suspenders.
        if self._pub_sock is not None:
            ready_payload = {
                "robot_id": self.robot_id,
                "command": "__ready__",
                "state": self.sdk.current_state,
                "timestamp": time.time(),
            }
            msg = encode_msg(topic_status(self.robot_id), ready_payload)
            for _ in range(10):
                self._pub_sock.send_multipart(msg)
                time.sleep(0.1)
            logger.info("Robot published %s (ready signal, 10×)", topic_status(self.robot_id))

    def _sensor_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._pub_sock is not None:
                payload = {
                    "robot_id": self.robot_id,
                    "state": self.sdk.current_state,
                    "timestamp": time.time(),
                }
                msg = encode_msg(topic_sensor(self.robot_id), payload)
                self._pub_sock.send_multipart(msg)
            time.sleep(SENSOR_INTERVAL_S)

    def _execute_command(self, payload: dict) -> None:
        command = payload.get("command", "")
        logger.info("Executing command: %s", command)
        cmd_map = {
            "forward": self.sdk.forward,
            "backward": self.sdk.backward,
            "left": self.sdk.left,
            "right": self.sdk.right,
            "stop": self.sdk.stop,
        }
        method = cmd_map.get(command)
        if method is None:
            logger.warning("Unknown command: %s", command)
            return
        method()
        if self._pub_sock is not None:
            status_payload = {
                "robot_id": self.robot_id,
                "command": command,
                "state": self.sdk.current_state,
                "timestamp": time.time(),
            }
            msg = encode_msg(topic_status(self.robot_id), status_payload)
            self._pub_sock.send_multipart(msg)
            logger.info("Robot published %s: %s", topic_status(self.robot_id), status_payload)

    # ── Main ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        if not self._register():
            return

        self._bind_pub()
        self._bind_pull()

        # Start background threads
        hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat"
        )
        hb_thread.start()

        sensor_thread = threading.Thread(
            target=self._sensor_loop, daemon=True, name="sensor"
        )
        sensor_thread.start()

        # Poll for mesh info on PULL socket (pushed by Cloud when User connects)
        logger.info("Waiting for mesh info via PUSH/PULL...")
        pull_poller = zmq.Poller()
        pull_poller.register(self._pull_sock, zmq.POLLIN)

        while self._mesh_info is None and not self._stop_event.is_set():
            socks = dict(pull_poller.poll(timeout=500))
            if self._pull_sock in socks:
                try:
                    self._mesh_info = self._pull_sock.recv_json()
                    logger.info("Received mesh info via PUSH/PULL: %s", self._mesh_info)
                except zmq.ZMQError:
                    break
        pull_poller.unregister(self._pull_sock)

        if self._mesh_info is None:
            logger.warning("Shutting down without mesh info")
            self.stop()
            return

        self._open_sub_sockets()

        # Now poll SUB (commands from User) and PULL (future mesh updates)
        sub_poller = zmq.Poller()
        if self._sub_user:
            sub_poller.register(self._sub_user, zmq.POLLIN)
        sub_poller.register(self._pull_sock, zmq.POLLIN)

        while not self._stop_event.is_set():
            try:
                socks = dict(sub_poller.poll(timeout=500))
            except zmq.ZMQError:
                break

            for sock in socks:
                if sock == self._sub_user:
                    try:
                        topic_str, payload_bytes = sock.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        continue
                    topic = decode_topic(topic_str)
                    payload = decode_payload(payload_bytes)
                    logger.info("Robot received %s: %s", topic, payload)
                    if topic == topic_command(self.robot_id):
                        self._execute_command(payload)

                elif sock == self._pull_sock:
                    try:
                        new_info = self._pull_sock.recv_json(zmq.NOBLOCK)
                    except zmq.Again:
                        continue
                    logger.info("Received updated mesh info via PUSH/PULL: %s", new_info)
                    self._mesh_info = new_info
                    # Close old SUB and reconnect
                    if self._sub_user:
                        self._sub_user.close(linger=0)
                        sub_poller.unregister(self._sub_user)
                        self._sub_user = None
                    self._open_sub_sockets()
                    if self._sub_user:
                        sub_poller.register(self._sub_user, zmq.POLLIN)

    def stop(self) -> None:
        logger.info("Robot '%s' stopping...", self.robot_id)
        self._stop_event.set()
        if self._pub_sock:
            self._pub_sock.close(linger=500)
        if self._pull_sock:
            self._pull_sock.close(linger=500)
        if self._sub_user:
            self._sub_user.close(linger=500)
        self._ctx.term()
        logger.info("Robot '%s' stopped", self.robot_id)


def main() -> None:
    import sys
    robot_id = sys.argv[1] if len(sys.argv) > 1 else "robot-1"
    client = RobotClient(robot_id)

    def _shutdown(signum, frame):
        client.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    client.run()


if __name__ == "__main__":
    main()

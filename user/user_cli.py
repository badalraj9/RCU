"""User CLI — a REPL for discovering and controlling robots via the pub/sub mesh.

Commands:
    list                  — show online robots from Cloud
    connect <robot_id>    — connect to a robot (spawns Player, forms mesh)
    send <command>        — publish a command to the connected robot
    quit                  — exit
"""

import shlex
import signal
import threading
import time
from typing import Optional

import zmq

from user.logger import get_logger
from user.protocol import (
    CONTROL_ADDR,
    SHARED_SECRET,
    decode_payload,
    decode_topic,
    encode_msg,
    topic_command,
    topic_processed,
    topic_status,
)

logger = get_logger("user_cli")


class UserCLI:
    """REPL client that connects to Cloud and joins the pub/sub mesh."""

    def __init__(self) -> None:
        self._ctx = zmq.Context()
        self._pub_sock: Optional[zmq.Socket] = None
        self._sub_sock: Optional[zmq.Socket] = None
        self._poller = zmq.Poller()
        self._connected_robot: Optional[str] = None
        self._stop_event = threading.Event()
        self._robot_ready = threading.Event()

    def _cloud_request(self, msg: dict) -> dict:
        sock = self._ctx.socket(zmq.REQ)
        sock.connect(CONTROL_ADDR)
        sock.send_json(msg)
        reply = sock.recv_json()
        sock.close(linger=0)
        return reply

    def cmd_list(self) -> None:
        reply = self._cloud_request({"type": "LIST_ROBOTS", "token": SHARED_SECRET})
        if reply.get("status") != "ok":
            print(f"Error: {reply.get('message')}")
            return
        robots = reply.get("robots", [])
        if not robots:
            print("No online robots.")
            return
        print(f"Online robots ({len(robots)}):")
        for r in robots:
            print(f"  {r['robot_id']} — {r['address']}:{r['pub_port']}")

    def cmd_connect(self, robot_id: str) -> None:
        reply = self._cloud_request({
            "type": "CONNECT_ROBOT",
            "robot_id": robot_id,
            "token": SHARED_SECRET,
        })
        if reply.get("status") != "ok":
            print(f"Error: {reply.get('message')}")
            return

        robot_pub_addr = reply["robot_pub_addr"]
        player_pub_addr = reply["player_pub_addr"]
        user_pub_port = reply["user_pub_port"]

        print(f"Connected to robot '{robot_id}'")
        print(f"  Robot PUB:   {robot_pub_addr}")
        print(f"  Player PUB:  {player_pub_addr}")
        print(f"  My PUB port: {user_pub_port}")

        # Bind our PUB socket
        self._pub_sock = self._ctx.socket(zmq.PUB)
        pub_addr = f"tcp://127.0.0.1:{user_pub_port}"
        self._pub_sock.bind(pub_addr)
        logger.info("User PUB bound to %s", pub_addr)

        # Open SUB sockets to Robot and Player (single socket, multiple connects)
        self._sub_sock = self._ctx.socket(zmq.SUB)
        self._sub_sock.connect(robot_pub_addr)
        self._sub_sock.setsockopt_string(zmq.SUBSCRIBE, topic_status(robot_id))

        self._sub_sock.connect(player_pub_addr)
        self._sub_sock.setsockopt_string(zmq.SUBSCRIBE, topic_processed(robot_id))

        self._connected_robot = robot_id
        self._poller.register(self._sub_sock, zmq.POLLIN)

        # Start subscriber printing thread (will set _robot_ready on __ready__)
        t = threading.Thread(target=self._print_subscriber, daemon=True)
        t.start()

        # Wait for Robot to confirm its SUB sockets are open (up to 1.5s)
        if self._robot_ready.wait(timeout=1.5):
            print("Robot ready — you can send commands.")
        else:
            print("Warning: Robot did not confirm readiness within 1.5s — commands may be lost.")

    def cmd_send(self, command: str) -> None:
        if self._connected_robot is None or self._pub_sock is None:
            print("Not connected to any robot. Use 'connect <robot_id>' first.")
            return
        payload = {
            "robot_id": self._connected_robot,
            "command": command,
            "timestamp": time.time(),
        }
        msg = encode_msg(topic_command(self._connected_robot), payload)
        self._pub_sock.send_multipart(msg)
        print(f"Sent command '{command}' to {self._connected_robot}")

    def _print_subscriber(self) -> None:
        """Continuously print incoming subscribed messages."""
        while not self._stop_event.is_set():
            try:
                socks = dict(self._poller.poll(timeout=500))
            except zmq.ZMQError:
                break
            if self._sub_sock in socks:
                try:
                    topic_str, payload_bytes = self._sub_sock.recv_multipart(zmq.NOBLOCK)
                except zmq.Again:
                    continue
                topic = decode_topic(topic_str)
                payload = decode_payload(payload_bytes)
                print(f"[RECV] {topic}: {payload}")

                # Detect Robot's "ready" signal
                if (topic == topic_status(self._connected_robot)
                        and payload.get("command") == "__ready__"):
                    self._robot_ready.set()

    def run(self) -> None:
        print("RCU Mesh User CLI")
        print("Commands: list, connect <robot_id>, send <command>, quit")
        print()

        def _shutdown(signum, frame):
            self.stop()

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

        while not self._stop_event.is_set():
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue

            parts = shlex.split(line)
            cmd = parts[0].lower()

            if cmd == "quit":
                break
            elif cmd == "list":
                self.cmd_list()
            elif cmd == "connect":
                if len(parts) < 2:
                    print("Usage: connect <robot_id>")
                else:
                    self.cmd_connect(parts[1])
            elif cmd == "send":
                if len(parts) < 2:
                    print("Usage: send <command>")
                else:
                    self.cmd_send(parts[1])
            else:
                print(f"Unknown command: {cmd}")

        self.stop()

    def stop(self) -> None:
        self._stop_event.set()
        if self._pub_sock:
            self._pub_sock.close(linger=500)
        if self._sub_sock:
            self._sub_sock.close(linger=500)
        self._ctx.term()
        logger.info("User CLI stopped")


def main() -> None:
    cli = UserCLI()
    cli.run()


if __name__ == "__main__":
    main()

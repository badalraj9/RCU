"""Cloud Service — main ZMQ REP loop.

Handles control-plane requests:
    REGISTER       robot → cloud
    HEARTBEAT      robot → cloud
    LIST_ROBOTS    user → cloud
    CONNECT_ROBOT  user → cloud

Port allocation uses a simple counter starting at BASE_PUB_PORT.
"""

import signal
from typing import Any

import zmq

from cloud_service.logger import get_logger
from cloud_service.player_manager import PlayerManager
from cloud_service.protocol import (
    BASE_PUB_PORT,
    CONTROL_ADDR,
    MSG_CONNECT_ROBOT,
    MSG_HEARTBEAT,
    MSG_LIST_ROBOTS,
    MSG_REGISTER,
    SHARED_SECRET,
    STATUS_ERROR,
    STATUS_OK,
)
from cloud_service.registry import Registry

logger = get_logger("cloud_server")


class CloudServer:
    """Cloud control-plane server (REP socket)."""

    def __init__(self) -> None:
        self.registry = Registry()
        self.player_manager = PlayerManager()
        self.registry.set_on_offline(self._on_robot_offline)

        self._next_port = BASE_PUB_PORT
        self._port_lock = __import__("threading").Lock()

        self._ctx = zmq.Context()
        self._rep_sock = self._ctx.socket(zmq.REP)
        self._running = False

    def _allocate_port(self) -> int:
        with self._port_lock:
            port = self._next_port
            self._next_port += 1
            return port

    def _on_robot_offline(self, robot_id: str) -> None:
        self.player_manager.kill(robot_id)

    def _handle_register(self, msg: dict) -> dict:
        robot_id = msg.get("robot_id", "")
        token = msg.get("token", "")
        if token != SHARED_SECRET:
            return {"status": STATUS_ERROR, "message": "Invalid or missing token"}
        if not robot_id:
            return {"status": STATUS_ERROR, "message": "Missing robot_id"}
        address = msg.get("address", "tcp://127.0.0.1:0")
        pub_port = self._allocate_port()
        push_port = self._allocate_port()
        error = self.registry.register(robot_id, address, pub_port, push_port)
        if error:
            return {"status": STATUS_ERROR, "message": error}
        return {"status": STATUS_OK, "pub_port": pub_port, "push_port": push_port}

    def _handle_heartbeat(self, msg: dict) -> dict:
        robot_id = msg.get("robot_id", "")
        mesh_info = self.registry.heartbeat(robot_id)
        return {"status": STATUS_OK, "mesh_info": mesh_info}

    def _handle_list_robots(self) -> dict:
        robots = self.registry.get_online_robots()
        return {"status": STATUS_OK, "robots": robots}

    def _handle_connect_robot(self, msg: dict) -> dict:
        robot_id = msg.get("robot_id", "")
        token = msg.get("token", "")
        if token != SHARED_SECRET:
            return {"status": STATUS_ERROR, "message": "Invalid or missing token"}

        entry = self.registry.get_robot(robot_id)
        if entry is None or entry["status"] != "online":
            return {
                "status": STATUS_ERROR,
                "message": f"Robot '{robot_id}' is not registered or is offline",
            }

        user_pub_port = self._allocate_port()
        user_addr = f"tcp://127.0.0.1:{user_pub_port}"

        player_pub_port = entry.get("player_pub_port")
        if player_pub_port is None:
            player_pub_port = self._allocate_port()
            self.registry.set_player_port(robot_id, player_pub_port)

        self.registry.set_user_info(robot_id, user_pub_port, user_addr)

        if not self.player_manager.is_running(robot_id):
            robot_addr = f"tcp://127.0.0.1:{entry['pub_port']}"
            error = self.player_manager.spawn(
                robot_id, player_pub_port, robot_addr, user_addr,
            )
            if error:
                return {"status": STATUS_ERROR, "message": error}

        robot_addr = f"tcp://127.0.0.1:{entry['pub_port']}"
        mesh_info: dict[str, Any] = {
            "robot_pub_addr": robot_addr,
            "player_pub_addr": f"tcp://127.0.0.1:{player_pub_port}",
            "user_pub_addr": user_addr,
            "user_pub_port": user_pub_port,
        }
        self.registry.set_mesh_info(robot_id, mesh_info)

        # Immediately push mesh info to Robot via dedicated PUSH/PULL channel
        push_port = entry.get("push_port")
        if push_port:
            robot_pull_addr = f"tcp://127.0.0.1:{push_port}"
            try:
                push_sock = self._ctx.socket(zmq.PUSH)
                push_sock.connect(robot_pull_addr)
                push_sock.send_json(mesh_info)
                push_sock.close(linger=1000)
                logger.info("Pushed mesh info to robot '%s' at %s", robot_id, robot_pull_addr)
            except zmq.ZMQError as exc:
                logger.error("Failed to push mesh info to '%s': %s", robot_id, exc)

        return {
            "status": STATUS_OK,
            "robot_pub_addr": robot_addr,
            "player_pub_addr": f"tcp://127.0.0.1:{player_pub_port}",
            "user_pub_port": user_pub_port,
        }

    def run(self) -> None:
        self._rep_sock.bind(CONTROL_ADDR)
        self._running = True
        logger.info("Cloud control-plane listening on %s", CONTROL_ADDR)

        def _shutdown(signum, frame):
            logger.info("Cloud received signal %s, shutting down", signum)
            self.stop()

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

        while self._running:
            try:
                msg = self._rep_sock.recv_json()
            except zmq.ZMQError:
                break

            msg_type = msg.get("type", "")
            logger.info("Request: %s (robot_id=%s)", msg_type, msg.get("robot_id", ""))

            if msg_type == MSG_REGISTER:
                response = self._handle_register(msg)
            elif msg_type == MSG_HEARTBEAT:
                response = self._handle_heartbeat(msg)
            elif msg_type == MSG_LIST_ROBOTS:
                response = self._handle_list_robots()
            elif msg_type == MSG_CONNECT_ROBOT:
                response = self._handle_connect_robot(msg)
            else:
                response = {"status": STATUS_ERROR, "message": f"Unknown type: {msg_type}"}

            try:
                self._rep_sock.send_json(response)
            except zmq.ZMQError:
                break

    def stop(self) -> None:
        self._running = False
        logger.info("Cloud server stopping...")
        self.player_manager.kill_all()
        self.registry.shutdown()
        self._rep_sock.close(linger=500)
        self._ctx.term()
        logger.info("Cloud server stopped")


def main() -> None:
    server = CloudServer()
    server.run()


if __name__ == "__main__":
    main()

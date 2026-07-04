"""Player — a cloud-side subprocess that processes robot sensor data.

Spawned by PlayerManager.  Binds its PUB socket, subscribes to robot/<id>/sensor,
republishes processed data as robot/<id>/processed, and also listens on
robot/<id>/status.

Usage:
    python -m cloud_service.player --robot-id <id> --pub-port <port> \
        --robot-addr <addr> --user-addr <addr>
"""

import argparse
import signal
import sys
import time

import zmq

from cloud_service.logger import get_logger
from cloud_service.protocol import (
    SENSOR_INTERVAL_S,
    decode_payload,
    decode_topic,
    encode_msg,
    topic_command,
    topic_processed,
    topic_sensor,
    topic_status,
)

logger = get_logger("player")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cloud-side Player process")
    parser.add_argument("--robot-id", required=True)
    parser.add_argument("--pub-port", required=True, type=int)
    parser.add_argument("--robot-addr", required=True)
    parser.add_argument("--user-addr", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    robot_id = args.robot_id
    pub_port = args.pub_port
    robot_addr = args.robot_addr
    user_addr = args.user_addr

    logger.info(
        "Starting Player for '%s' — pub port %s, robot %s, user %s",
        robot_id, pub_port, robot_addr, user_addr,
    )

    ctx = zmq.Context()

    # PUB socket (our broadcast address)
    pub_sock = ctx.socket(zmq.PUB)
    pub_addr = f"tcp://127.0.0.1:{pub_port}"
    pub_sock.bind(pub_addr)
    logger.info("Player PUB bound to %s", pub_addr)

    # SUB sockets to Robot and User
    robot_pub_addr = robot_addr
    sub_robot = ctx.socket(zmq.SUB)
    sub_robot.connect(robot_pub_addr)
    # Subscribe to sensor and status topics from robot
    sub_robot.setsockopt_string(zmq.SUBSCRIBE, topic_sensor(robot_id))
    sub_robot.setsockopt_string(zmq.SUBSCRIBE, topic_status(robot_id))
    logger.info("Player SUB -> Robot at %s", robot_pub_addr)

    sub_user = ctx.socket(zmq.SUB)
    sub_user.connect(user_addr)
    sub_user.setsockopt_string(zmq.SUBSCRIBE, topic_command(robot_id))
    logger.info("Player SUB -> User at %s", user_addr)

    # Poller for incoming messages
    poller = zmq.Poller()
    poller.register(sub_robot, zmq.POLLIN)
    poller.register(sub_user, zmq.POLLIN)

    shutdown_flag = False

    def _handle_sigterm(signum, frame):
        nonlocal shutdown_flag
        logger.info("Player received SIGTERM, shutting down")
        shutdown_flag = True

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    while not shutdown_flag:
        try:
            socks = dict(poller.poll(timeout=500))
        except zmq.ZMQError:
            break

        for sock in socks:
            try:
                topic_str, payload_bytes = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                continue

            topic = decode_topic(topic_str)
            payload = decode_payload(payload_bytes)
            logger.info("Player received %s: %s", topic, payload)

            if topic == topic_sensor(robot_id):
                # "Process" sensor data — add a status field
                value = payload.get("state", 0.0)
                processed = dict(payload)
                processed["status"] = "normal" if 0 <= value <= 100 else "warning"
                msg = encode_msg(topic_processed(robot_id), processed)
                pub_sock.send_multipart(msg)
                logger.info("Player published %s: %s", topic_processed(robot_id), processed)

            # robot/<id>/status messages are just printed (already logged above)

    # Clean shutdown
    pub_sock.close()
    sub_robot.close()
    sub_user.close()
    ctx.term()
    logger.info("Player for '%s' exited cleanly", robot_id)


if __name__ == "__main__":
    main()

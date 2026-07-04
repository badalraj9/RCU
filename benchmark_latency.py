"""Latency benchmark for pub/sub mesh.

WARNING: This benchmark measures same-process ZMQ overhead only; it does
NOT represent cross-process or real-system latency.  All sockets live in
one process/thread — no Cloud, Robot, Player, or User subprocesses.

Robot publishes N sensor messages with a timestamp in the payload;
a subscriber measures receive-time minus sent-time, prints min/max/avg latency.

Usage:
    python benchmark_latency.py [--count 100]
"""

import argparse
import time

import zmq

from cloud_service.protocol import (
    encode_msg,
    decode_payload,
    topic_sensor,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pub/sub latency benchmark")
    parser.add_argument("--count", type=int, default=100, help="Number of messages")
    args = parser.parse_args()

    ctx = zmq.Context()

    pub_port = 19999
    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://127.0.0.1:{pub_port}")

    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://127.0.0.1:{pub_port}")
    sub.setsockopt_string(zmq.SUBSCRIBE, topic_sensor("benchmark"))
    # Warmup
    time.sleep(0.5)
    for _ in range(5):
        pub.send_multipart(encode_msg(topic_sensor("benchmark"), {"state": 0}))
        time.sleep(0.05)

    latencies: list[float] = []

    for i in range(args.count):
        sent_ns = time.perf_counter_ns()
        payload = {"state": float(i), "timestamp_ns": sent_ns}
        pub.send_multipart(encode_msg(topic_sensor("benchmark"), payload))

        if sub.poll(timeout=2000):
            _, payload_bytes = sub.recv_multipart()
            received = decode_payload(payload_bytes)
            recv_ns = time.perf_counter_ns()
            sent_ns_from_msg = received.get("timestamp_ns", sent_ns)
            latency_us = (recv_ns - sent_ns_from_msg) / 1000.0
            latencies.append(latency_us)
        else:
            print(f"Warning: message {i} not received within timeout")

    pub.close(linger=0)
    sub.close(linger=0)
    ctx.term()

    if not latencies:
        print("No messages received — benchmark failed.")
        return

    avg_us = sum(latencies) / len(latencies)
    sorted_lat = sorted(latencies)

    print(f"\nLatency benchmark ({len(latencies)} messages):")
    print(f"  Min:    {min(latencies):.1f} µs")
    print(f"  Max:    {max(latencies):.1f} µs")
    print(f"  Avg:    {avg_us:.1f} µs")
    print(f"  Median: {sorted_lat[len(sorted_lat) // 2]:.1f} µs")


if __name__ == "__main__":
    main()

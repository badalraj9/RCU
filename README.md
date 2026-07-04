# RCU — Robot-Cloud-User Triangle Pub/Sub Mesh

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONTROL PLANE (REQ/REP)                   │
│                                                                  │
│   Robot ──REGISTER/HEARTBEAT──▶ Cloud                           │
│   User  ──LIST/CONNECT───────▶ Cloud                           │
└─────────────────────────────────────────────────────────────────┘

                    ┌──────────┐
                    │  Cloud   │  (control plane only)
                    └────┬─────┘
                         │ spawns
                    ┌────▼─────┐
                    │  Player  │  (subprocess)
                    └────┬─────┘
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
 ┌──────┐           ┌────────┐           ┌──────┐
 │ Robot│◄─────────►│ Player │◄─────────►│ User │
 │ PUB  │   DATA    │  PUB   │   DATA    │ PUB  │
 │ SUB  │   PLANE   │  SUB   │   PLANE   │ SUB  │
 └──────┘  PUB/SUB  └────────┘  PUB/SUB  └──────┘

  Data-plane: triangular brokerless PUB/SUB mesh
  Control-plane: Cloud-mediated REQ/REP (not in data path)
```

Each entity binds one PUB socket (its broadcast address) and opens SUB sockets
directly connecting to the other two peers' PUB addresses. Cloud is only in the
control path — it mediates discovery but never touches data messages.

## How to Run

### Step 0 — Set the shared secret

All four processes need the same `RCU_SECRET` environment variable. Run this in
**every terminal** before starting the respective process:

**Linux / macOS:**
```bash
export RCU_SECRET=my-secret-value
```

**Windows (cmd.exe):**
```cmd
set RCU_SECRET=my-secret-value
```

**Windows (PowerShell):**
```powershell
$env:RCU_SECRET = "my-secret-value"
```

The process will refuse to start if `RCU_SECRET` is not set. Use any value you
choose — it is checked on all control-plane requests to prevent unauthorized
registrations or connections.

---

Open **4 terminal windows**:

### Terminal 1 — Cloud Service
```bash
python -m cloud_service
```

### Terminal 2 — Robot
```bash
python -m robot robot-1
```

### Terminal 3 — Robot (optional, second robot)
```bash
python -m robot robot-2
```

### Terminal 4 — User CLI
```bash
python -m user
```

### Demo Flow

1. **Terminal 1**: Cloud starts, waits for requests.
2. **Terminal 2**: Robot registers, starts heartbeats & publishing sensor data.
3. **Terminal 4 (User CLI)**:
   ```
   > list
   Online robots (1):
     robot-1 — tcp://127.0.0.1:6000
   > connect robot-1
   Connected to robot 'robot-1'
   [RECV] robot/robot-1/processed: {"state": 50.0, "status": "normal", ...}
   > send forward
   Sent command 'forward' to robot-1
   [RECV] robot/robot-1/status: {"command": "forward", "state": 55.0, ...}
   > quit
   ```
4. **Terminal 2**: Shows sensor publishes, received commands, and executed SDK actions.

The triangle mesh forms automatically when `connect` is issued:
- Cloud spawns a Player subprocess.
- Robot receives mesh info immediately via a dedicated ZMQ PUSH/PULL channel (no heartbeat wait), opens SUBs to Player & User.
- User opens SUBs to Robot & Player.
- All three are now directly connected — Cloud is out of the data path.

## Topics

| Topic Pattern | Publisher | Subscribers | Payload |
|---|---|---|---|
| `robot/<id>/sensor` | Robot | Player | `{"robot_id": ..., "state": <float>, "timestamp": ...}` |
| `robot/<id>/command` | User | Robot, Player | `{"robot_id": ..., "command": "forward", "timestamp": ...}` |
| `robot/<id>/status` | Robot | User, Player | `{"robot_id": ..., "command": ..., "state": ..., "timestamp": ...}` |
| `robot/<id>/processed` | Player | User | `{"robot_id": ..., "state": ..., "status": "normal"|"warning", "timestamp": ...}` |

All messages are ZMQ multipart: frame 0 = topic string (utf-8), frame 1 = JSON payload (utf-8).

### Example Messages

**Sensor** (`robot/robot-1/sensor`):
```json
{"robot_id": "robot-1", "state": 52.3, "timestamp": 1712345678.901}
```

**Command** (`robot/robot-1/command`):
```json
{"robot_id": "robot-1", "command": "forward", "timestamp": 1712345679.123}
```

**Status** (`robot/robot-1/status`):
```json
{"robot_id": "robot-1", "command": "forward", "state": 57.3, "timestamp": 1712345679.124}
```

**Processed** (`robot/robot-1/processed`):
```json
{"robot_id": "robot-1", "state": 52.3, "status": "normal", "timestamp": 1712345678.902}
```

## Assumptions

- **All processes run on localhost.** IPs are hardcoded as `127.0.0.1` throughout.
  In production, Cloud would resolve hostnames/addresses from the REGISTER payload.
- **Port allocation is a simple incrementing counter.** No port reuse or garbage
  collection. Starting at `BASE_PUB_PORT` (6000) and counting up. The Cloud control
  port is fixed at 5555.
- **No message persistence or replay.** ZMQ PUB/SUB delivers messages to
  subscribers connected at publish time. Late subscribers don't receive past
  messages — this is a known ZMQ limitation and accepted as out of scope.
- **Mesh info is delivered instantly via a dedicated PUSH/PULL channel, not
  piggybacked on heartbeats.** When a User connects, Cloud opens a PUSH socket
  connected to Robot's PULL (`push_port`) and pushes the mesh info immediately.
  This avoids the ~2s command-loss window that would exist if Robot had to wait
  for its next heartbeat response. Heartbeats remain for liveness detection only.
- **Robot publishes a `__ready__` status once its SUB sockets are open.** User
  CLI waits up to 1.5s for this signal before printing a readiness confirmation.
  If the signal is missed (e.g. due to ZMQ's first-pub-loss), a warning is shown
  but commands are still sent and will be delivered once the SUB connection
  completes asynchronously.
- **Duplicate `robot_id` while online → rejection.** A robot reconnecting after
  being marked offline (heartbeat timeout) may re-register freely.
- **`protocol.py` and `logger.py` are duplicated** in each of the three packages
  (`cloud_service/`, `robot/`, `user/`). This is an explicit tradeoff (~30 lines × 3)
  to respect the required folder structure without a `shared/` package.
- **Heartbeat timeout is 5 seconds**, with heartbeats sent every 2 seconds.
  This gives 2–3 missed heartbeats before the robot is marked offline.
- **Player processes are killed via SIGTERM** when a robot goes offline or Cloud
  shuts down. If SIGTERM doesn't work within 5 seconds, SIGKILL is used.
- **Ctrl+C on any process** triggers cleanup via signal handlers. ZMQ sockets
  and contexts are closed with `linger=0` to avoid hangs.

## Running Tests

```bash
# Unit tests (registry)
python -m pytest cloud_service/test_registry.py -v

# Integration test (cloud + robot + user in-process)
python -m pytest test_integration.py -v -s
```

## Running Benchmark

```bash
python benchmark_latency.py --count 100
```

## Security (Bonus)

A shared-secret token (`SHARED_SECRET` in `protocol.py`) is checked on all
control-plane requests (`REGISTER` and `CONNECT_ROBOT`). Requests without a
valid token are rejected with an error message.

In production, the next steps would be:
- TLS/mTLS on all ZMQ sockets (both control-plane REQ/REP and data-plane PUB/SUB).
- Per-robot API keys instead of a single shared secret.
- Certificate-based authentication for robot identity.
- The shared secret should come from an environment variable or a secrets manager,
  not from source code.

## Scalability (Bonus — Written Design, Not Implemented)

To scale this system to N robots and M users:

1. **Registry**: Replace the in-memory `dict` with Redis or Postgres. This allows
   multiple Cloud instances to share state. TTL-based expiry (Redis `EXPIRE`)
   would replace the background monitor thread.
2. **Port allocation**: Move to a proper port allocation service or use a
   single well-known PUB port per entity (relying on ZMQ's ability to have
   multiple subscribers on one port). Alternatively, use a relay/proxy pattern
   instead of direct mesh connections.
3. **Cloud horizontal scaling**: Cloud instances would sit behind a discovery
   layer (e.g., consistent hash ring or a simple load balancer). Robots connect
   to any Cloud instance; the registry backend ensures consistency.
4. **Connection limits**: With N robots and M users, each entity opens N-1+M
   SUB sockets. At large scale, this becomes unwieldy. A forwarder/relay
   process (per-robot or per-group) would reduce the connection fan-out.
5. **Player scaling**: Each robot gets one Player. At scale, Players could be
   pooled and multiplexed, or the processing logic could be moved to a stream
   processor (e.g., Kafka Streams, Flink) that subscribes to all sensor topics.

## Project Structure

```
repo/
├── cloud_service/
│   ├── __init__.py
│   ├── __main__.py          # python -m cloud_service
│   ├── logger.py            # shared (duplicated)
│   ├── player.py            # subprocess entry point
│   ├── player_manager.py    # spawn/kill Player subprocesses
│   ├── protocol.py          # shared (duplicated)
│   ├── registry.py          # thread-safe in-memory registry
│   ├── server.py            # ZMQ REP loop
│   └── test_registry.py     # unit tests
├── robot/
│   ├── __init__.py
│   ├── __main__.py          # python -m robot [robot_id]
│   ├── logger.py            # shared (duplicated)
│   ├── protocol.py          # shared (duplicated)
│   ├── robot_client.py      # main robot client
│   └── robot_sdk.py         # fake JetBot SDK
├── user/
│   ├── __init__.py
│   ├── __main__.py          # python -m user
│   ├── logger.py            # shared (duplicated)
│   ├── protocol.py          # shared (duplicated)
│   └── user_cli.py          # REPL CLI
├── test_integration.py      # root-level integration test
├── benchmark_latency.py     # root-level latency benchmark
├── requirements.txt
└── README.md
```

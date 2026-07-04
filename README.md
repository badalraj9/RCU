# RCU — Robot-Cloud-User Triangle Pub/Sub Mesh

**Zero-broker, direct peer-to-peer pub/sub mesh for controlling robots in real time.** Cloud handles discovery only — every byte of sensor data, status, and command flows directly between peers over ZMQ.

---

## Quick Start

```bash
# Terminal 1
export RCU_SECRET=my-secret
python -m cloud_service

# Terminal 2
export RCU_SECRET=my-secret
python -m robot robot-1

# Terminal 3
export RCU_SECRET=my-secret
python -m user
```

Then in the User CLI:
```
> list
> connect robot-1
> send forward
> send stop
```

---

## Architecture

```
                    ┌──────────┐           CONTROL PLANE (REQ/REP)
                    │  Cloud   │           ───────────────────────
                    └────┬─────┘           Robot → Cloud: REGISTER, HEARTBEAT
                         │                 User  → Cloud: LIST, CONNECT
                    ┌────▼─────┐
                    │  Player  │           Cloud spawns a Player per connected robot
                    └────┬─────┘
                         │
     ┌───────────────────┼───────────────────┐
     │                   │                   │
     ▼                   ▼                   ▼
  ┌──────┐          ┌────────┐          ┌──────┐
  │ Robot│◄────────►│ Player │◄────────►│ User │
  │ PUB  │   DATA   │  PUB   │   DATA   │ PUB  │
  │ SUB  │   PLANE  │  SUB   │   PLANE  │ SUB  │
  └──────┘  PUB/SUB └────────┘  PUB/SUB └──────┘
```

**Two planes, one triangle:**

| Plane | Transport | What flows |
|---|---|---|
| **Control** | ZMQ REQ/REP | Registration, heartbeats, discovery |
| **Data** | ZMQ PUB/SUB | Sensor readings, commands, status, processed data |

Cloud never touches a data message. It mediates discovery, then gets out of the way.

---

## How It Works

### Discovery flow

```
┌──────┐                  ┌───────┐                  ┌──────┐
│ User │                  │ Cloud │                  │Robot │
└──┬───┘                  └───┬───┘                  └──┬───┘
   │                          │                         │
   │                          │◄──── REGISTER ──────────│
   │                          │──── pub_port + ────────►│
   │                          │     push_port           │
   │                          │                         │
   │── LIST_ROBOTS ──────────►│                         │
   │◄──── robots[] ──────────│                         │
   │                          │                         │
   │── CONNECT_ROBOT ───────►│                         │
   │                          │── PUSH mesh_info ──────►│  (dedicated PUSH/PULL)
   │                          │                         │  (no heartbeat wait)
   │◄── robot/player addrs ──│                         │
   │                          │                         │
   │◄═══════════ PUB/SUB mesh ─════════════════════════►│  (direct, no Cloud)
```

### Data flow

```
Robot PUB ──sensor─────▶ Player SUB ──processed──▶ User SUB
    │                                                   
    │────status────────▶ User SUB                      
    │                   Player SUB                     
    │                                                   
User PUB ──command────▶ Robot SUB                      
                        Player SUB                     
```

---

## How to Run

### 1. Set the shared secret

Every terminal needs `RCU_SECRET` set before starting:

**Linux / macOS**
```bash
export RCU_SECRET=my-secret-value
```

**Windows (cmd)**
```cmd
set RCU_SECRET=my-secret-value
```

**Windows (PowerShell)**
```powershell
$env:RCU_SECRET = "my-secret-value"
```

The process will refuse to start without it.

### 2. Start the Cloud

```bash
python -m cloud_service
```

Cloud binds its control-plane REP socket on `tcp://127.0.0.1:5555` and waits.

### 3. Start a Robot

```bash
python -m robot robot-1
```

Robot registers, binds PUB + PULL, starts heartbeats and sensor publishing, then waits for a User to connect.

You can add more robots:
```bash
python -m robot robot-2
```

### 4. Start the User CLI

```bash
python -m user
```

### Demo walkthrough

```
> list
Online robots (1):
  robot-1 — tcp://127.0.0.1:6000

> connect robot-1
Connected to robot 'robot-1'
  Robot PUB:   tcp://127.0.0.1:6000
  Player PUB:  tcp://127.0.0.1:6003
  My PUB port: 6002
Robot ready — you can send commands.

> send forward
Sent command 'forward' to robot-1
[RECV] robot/robot-1/status: {'command': 'forward', 'state': 55.0, ...}

> send stop
Sent command 'stop' to robot-1
[RECV] robot/robot-1/status: {'command': 'stop', 'state': 50.0, ...}

> quit
```

---

## Topics

| Topic | Publisher | Subscribers | Payload |
|---|---|---|---|
| `robot/<id>/sensor` | Robot | Player | `state`, `timestamp` |
| `robot/<id>/command` | User | Robot, Player | `command`, `timestamp` |
| `robot/<id>/status` | Robot | User, Player | `command`, `state`, `timestamp` |
| `robot/<id>/processed` | Player | User | `state`, `status`, `timestamp` |

All messages are ZMQ multipart: frame 0 = topic string (utf-8), frame 1 = JSON payload (utf-8).

### Example: sensor
```json
{"robot_id": "robot-1", "state": 52.3, "timestamp": 1712345678.901}
```

### Example: command
```json
{"robot_id": "robot-1", "command": "forward", "timestamp": 1712345679.123}
```

### Example: status
```json
{"robot_id": "robot-1", "command": "forward", "state": 57.3, "timestamp": 1712345679.124}
```

### Example: processed
```json
{"robot_id": "robot-1", "state": 52.3, "status": "normal", "timestamp": 1712345678.902}
```

---

## Design Decisions

### Mesh info delivery: PUSH/PULL, not heartbeat piggyback

Originally, mesh info was attached to the heartbeat response. That meant Robot's SUB sockets didn't open until the next heartbeat tick — up to 2 seconds of dropped commands. Now Cloud pushes mesh info over a dedicated ZMQ PUSH/PULL channel the instant CONNECT_ROBOT fires. Heartbeats are liveness-only.

### Ready signal: retry-publish, no magic sleep

Robot publishes `__ready__` up to 10 times (100ms apart) after opening its SUB sockets, because ZMQ's async SUB connect means the first publish often lands on no one. The User CLI waits up to 1.5s for it with a clear timeout warning. No arbitrary `time.sleep` on the critical path.

### Authentication

A shared secret (`RCU_SECRET`) gates every control-plane request. It's required — no fallback, no default. All three processes must agree on the same value. Next step would be per-robot API keys + TLS.

---

## Assumptions

| # | Assumption |
|---|---|
| 1 | **All processes on localhost.** IPs hardcoded as `127.0.0.1`. In production, Cloud resolves the REGISTER address. |
| 2 | **Port allocation = incrementing counter.** Starts at 6000, counts up. No reuse, no GC. Control port fixed at 5555. |
| 3 | **No message persistence/replay.** ZMQ PUB/SUB delivers to connected subscribers only. Late joiners get nothing — accepted tradeoff. |
| 4 | **Duplicate robot_id while online → rejected.** Offline robots may re-register freely. |
| 5 | **protocol.py and logger.py are duplicated** 3× (~30 lines each). Explicit tradeoff to avoid a `shared/` package. |
| 6 | **Heartbeat timeout = 5s**, sent every 2s (2–3 missed → offline). |
| 7 | **Player killed via SIGTERM → SIGKILL** after 5s if unresponsive. |

---

## Tests

```bash
# Unit tests (registry)
python -m pytest cloud_service/test_registry.py -v

# Integration test (in-process cloud + robot + user)
python -m pytest test_integration.py -v -s
```

## Benchmark

```bash
python benchmark_latency.py --count 100
```

---

## Project Structure

```
.
├── cloud_service/        # Cloud control plane + Player subprocess
│   ├── server.py         #   ZMQ REP loop — REGISTER, HEARTBEAT, LIST, CONNECT
│   ├── registry.py       #   Thread-safe in-memory robot registry
│   ├── player.py         #   Subprocess: subscribes sensor, publishes processed
│   ├── player_manager.py #   Spawn/kill Player lifecycle
│   ├── protocol.py       #   Shared constants, topic helpers (duplicated)
│   ├── logger.py         #   Shared logging config (duplicated)
│   └── test_registry.py  #   Unit tests
├── robot/                # Robot client
│   ├── robot_client.py   #   Registration, mesh join, command execution
│   ├── robot_sdk.py      #   Fake JetBot SDK (forward/backward/left/right/stop)
│   ├── protocol.py       #   Byte-identical copy
│   └── logger.py         #   Byte-identical copy
├── user/                 # User CLI
│   ├── user_cli.py       #   REPL: list, connect, send, quit
│   ├── protocol.py       #   Byte-identical copy
│   └── logger.py         #   Byte-identical copy
├── test_integration.py   # Integration test
├── benchmark_latency.py  # Latency benchmark
├── requirements.txt
└── README.md
```

---

## Scaling (Design Notes)

To go from localhost to N robots and M users:

1. **Registry** → Redis/Postgres with TTL expiry (replaces in-memory `dict` + monitor thread).
2. **Port allocation** → Single well-known per-entity port, or a relay proxy.
3. **Cloud HA** → Stateless Cloud instances behind a discovery layer; shared registry backend.
4. **Connection fan-out** → Each entity opens N+M SUB sockets. At scale, add a forwarder per group.
5. **Player scaling** → Pooled Players or stream processor (Kafka Streams, Flink).

---

## Security

- `RCU_SECRET` env var gates every control-plane request.
- Next: TLS/mTLS on all sockets, per-robot API keys, certificate-based identity.

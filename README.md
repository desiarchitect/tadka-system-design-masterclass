# Tadka: System Design Masterclass

The practical case study for the Desi Architect System Design Masterclass. Each lesson that needs a lab has its own branch. You check out the branch, inspect what is there, push traffic or failures at it, diagnose what you see, then compare it with the next branch.

**Inspect → Break → Diagnose → Evolve**

## Branch: `lesson-24-cache-stampede`

Lesson 24. Start with [the Lesson 24 contract](docs/lessons/lesson-24-contract.md).
Reproduces a cache stampede, then adds in-process single-flight request coalescing and TTL jitter.
Builds on `lesson-23-caching-strategies`. Run `node scripts/verify-cache-stampede.mjs` for this lesson's checks.

Earlier labs that are still on this branch, each with its own contract and verifier: lessons [10](docs/lessons/lesson-10-contract.md), [11](docs/lessons/lesson-11-contract.md), [12](docs/lessons/lesson-12-contract.md), [14](docs/lessons/lesson-14-contract.md), [15](docs/lessons/lesson-15-contract.md), [16](docs/lessons/lesson-16-contract.md), [18](docs/lessons/lesson-18-contract.md), [23](docs/lessons/lesson-23-contract.md). Run one lesson's stack at a time, because they share ports.

## Inherited branch: `lesson-08-realtime-sse`

Start with [the Lesson 08 contract](docs/lessons/lesson-08-contract.md).
This evolves Lesson 07 with persisted status versions and SSE replay.
Run `node scripts/verify-realtime.mjs` as well as the inherited API checks.
No WebSocket or broker is added. No frontend or production authorization is claimed.

## Inherited branch: `lesson-07-api-contracts`

Use [the Lesson 07 contract](docs/lessons/lesson-07-contract.md) for setup,
commands, fault injection, assertions and limitations. The create path now uses
an atomic database-backed idempotency key and has no artificial sleep. It adds
bounded offset and cursor pages. Run `node scripts/verify-api-contracts.mjs`.

The historical baseline notes below describe Lesson 01, not this branch's create path.

## Historical baseline: `lesson-01-baseline`

Lesson 01, *Inspect & Break: The Tadka Order API Under a Traffic Spike*.

This branch proves one thing: an API can become slow because requests wait for a database connection, even when the database command itself has not become slower. It is a small experiment, not a food-delivery app.

| Part | What it is |
|---|---|
| `src/Tadka.OrderApi` | One ASP.NET Core endpoint, `POST /orders`. One instance. Connection pool size 10, set explicitly. |
| `database/init.sql` | One `orders` table. |
| `docker-compose.yml` | PostgreSQL 16 on port 5433. |
| `load-tests/order-spike.js` | k6 with a fixed arrival rate. |
| `scripts/` | Database activity monitor and reset. |
| `docs/lessons/` | The language-neutral lesson contract. |
| `ports/` | Learner ports in other languages (empty at first). |

### What to inspect first

1. **Pool settings:** `src/Tadka.OrderApi/appsettings.json`, the connection string. `Maximum Pool Size=10` is the most connections this API instance will use at once. `Timeout=15` is how many seconds a request waits for a free pooled connection before it fails. This is the application-side pool. It is a different setting from PostgreSQL's own `max_connections`.
2. **No artificial delay in this lesson:** the pool-hold demo belongs to lesson 01. Here `Demo` only names the environment that enables the response-drop fault injection.

### The theory you will test

```
Pool size = 10 connections
Hold time = 200 ms
1000 / 200 = 5 operations per second per connection
10 × 5     = 50 operations per second (theoretical, pool-bound)
```

This is a theoretical limit for the pool under those assumptions. It is not the application's capacity.

## Prerequisites

- Docker
- .NET 10 SDK
- Node 22+ (for the `scripts/verify-*.mjs` checks)
- k6 (only for the historical Lesson 01 runs below)

## Run the experiment

Use three terminal panes.

**Pane 3, database (start first):**

```bash
docker compose up -d --wait
./scripts/pg-activity.sh          # or: ./scripts/pg-activity.ps1
```

**Pane 2, API (200 ms hold):**

```bash
cd src/Tadka.OrderApi
dotnet run -c Release --launch-profile demo
```

The first log line prints the settings in use: `max_pool_size`, `pool_timeout_s`, `hold_ms`. Check them before every run.

**Pane 1, load:**

```bash
k6 run -e RATE=20  -e DURATION=10s load-tests/order-spike.js
k6 run -e RATE=40  -e DURATION=10s load-tests/order-spike.js
k6 run -e RATE=100 -e DURATION=30s load-tests/order-spike.js
```

**Run 4, the 20 ms hold.** Stop the API (Ctrl+C) and start it again with a shorter hold:

```bash
# bash
HOLD_MS=20 dotnet run -c Release --launch-profile demo
# PowerShell
$env:HOLD_MS=20; dotnet run -c Release --launch-profile demo
```

Then:

```bash
k6 run -e RATE=100 -e DURATION=15s load-tests/order-spike.js
```

Remove the variable afterwards (`Remove-Item Env:HOLD_MS` in PowerShell) so the next start uses 200 ms again.

## What to look at

| Where | Read this |
|---|---|
| k6 summary | `http_reqs`, `http_req_failed`, `http_req_duration`, `checks_succeeded`, and `dropped_iterations` if it appears |
| API log | `wait_ms` and `db_ms` of the same request, side by side. Failures show `status=503` and the pool exception. |
| Database pane | `active` and `idle` client sessions on PostgreSQL |

Things that are easy to misread:

- **k6 `http_reqs .../s` is not the arrival rate.** It is total requests divided by the whole test time, including the time spent waiting for slow requests to finish after arrivals stop. Compare `http_reqs` (the count) with `RATE × DURATION` instead.
- **The database pane cannot show the pool's queue.** `pg_stat_activity` lists sessions on the PostgreSQL server. Requests waiting inside the API for a pooled connection are not sessions yet, so they never appear there. That is why `wait_ms` is measured in the application.
- **The query filters to `backend_type = 'client backend'`.** Without it, PostgreSQL's autovacuum worker shows up as an extra active session during heavy inserts, and the count can read 11 with a pool of 10.
- **The first request after a start is slower.** The pool starts empty, so the first requests include opening a real connection to PostgreSQL inside `wait_ms`.
- **Results are from your machine.** They are not a benchmark.

## Reset

Between runs, empty the table:

```bash
./scripts/reset.sh                # or: ./scripts/reset.ps1
```

To throw everything away: `docker compose down`. There is no volume, so the next `up` starts clean.

## Build it in your own language, with AI

The reference is .NET, but the experiment is not. You can rebuild this lab in Java, Node.js, Python, Go or anything else and run the same k6 script against it.

The language-neutral spec is [`docs/lessons/lesson-01-contract.md`](docs/lessons/lesson-01-contract.md): endpoint, SQL, pool settings, where each timer starts and stops, and the log format. Ports go in `ports/<language>-<framework>/` and reuse the database, load tests and scripts from the root.

The repo is set up for AI coding assistants:

| Tool | What it reads |
|---|---|
| Any assistant | `AGENTS.md`, the shared rules |
| Claude Code | `CLAUDE.md`, plus the `port-lab` and `run-experiment` skills, the `lab-reviewer` agent and a session-start hook in `.claude/` |
| GitHub Copilot | `.github/copilot-instructions.md` |
| Cursor | `.cursor/rules/tadka.mdc` |

With Claude Code, for example:

```text
Port this lab to Java with Spring Boot and HikariCP.
Review my port with the lab-reviewer agent.
Run the experiment against ports/java-spring.
```

Two things to keep in mind. First, the assistant can be wrong about how your database driver's pool behaves, especially the acquire timeout, so check its claims against the driver's documentation. Second, the point is the experiment. If your port's numbers differ from the reference, find out why. That investigation is the lesson.

## Not on this branch, on purpose

Microservices, Kubernetes, dashboards, production authentication, WebSockets and a frontend. The core labs do not need them. Not here yet: Kafka (Lesson 31).

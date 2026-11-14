# Tadka: System Design Masterclass

The pre-built case study for the Desi Architect System Design Masterclass.
Check out a lesson branch, inspect its contract, reproduce the problem and compare
the change. This is teaching code, not a production food-ordering service.

## Available branches

Each branch carries its own instructions. Do not mix the schema or commands from
different lessons.

| Branch | What you inspect | Start here after switching |
|---|---|---|
| `lesson-01-baseline` | Order API, PostgreSQL and application-side connection-pool wait | `docs/lessons/lesson-01-contract.md` |
| `lesson-05-networking` | HTTP/1.1 and HTTP/2 negotiation, fresh versus reused connections | `docs/lessons/lesson-05-contract.md` |
| `lesson-07-api-contracts` | Database-backed idempotency, lost responses and cursor pages | `docs/lessons/lesson-07-contract.md` |
| `lesson-08-realtime-sse` | Persisted status versions, SSE framing and reconnect replay | `docs/lessons/lesson-08-contract.md` |
| `lesson-10-modular-boundaries` | Explicit Catalog/Orders module contracts, schema ownership and receipt snapshots | `docs/lessons/lesson-10-contract.md` |
| `lesson-11-api-gateway-bff` | Edge Gateway routing, caller role checks, correlation IDs and resilient BFF shaping | `docs/lessons/lesson-11-contract.md` |

Lessons 02, 03, 04, 06 and 09 are board lessons and do not need branches.
Later module branches (Module 3+) are not published yet.

## Get a lesson

```text
git clone https://github.com/desiarchitect/tadka-system-design-masterclass.git
cd tadka-system-design-masterclass
git fetch origin
git switch lesson-05-networking
dotnet run -c Release --project src/Tadka.NetworkingLab
```

The networking probe needs .NET 10 only. It starts and stops its own loopback
HTTPS server and asserts connection reuse. It does not need PostgreSQL or Docker.
Request timings are observations from your machine, not promised benchmarks.
HTTP/3 is explained in the lesson but is not implemented in the probe.

For Lessons 07, 08, 10 and 11, install .NET 10, Docker and Node 22+, switch to the branch,
then follow its contract. Run one lesson API/database at a time because they share
ports 5080 and 5433 (Lesson 11 also uses port 5081 for `Tadka.GatewayBff`). Stop only that lesson's Compose project before switching.
The databases are disposable. API-only restart preserves data; database teardown
resets it.

The setup-check command for the Start Here video is still planned. `main` is an
index, not a runnable application. Do not record that setup check as available.

## Evolution and limits

Networking and API contracts each start from the baseline. The SSE branch builds
on API contracts. Module 2 (`lesson-10-modular-boundaries` and `lesson-11-api-gateway-bff`)
builds on `lesson-08-realtime-sse` by splitting `Catalog` and `Orders` inside the modular
monolith and adding `Tadka.GatewayBff` in front of `Tadka.OrderApi`. Networking does
not silently change the Order API.

Lessons 07, 08, 10 and 11 intentionally keep the artificial SQL sleep removed.
Their assertion scripts first reproduce unsafe or broken behaviour, then verify the fix.
The labs have been run repeatedly from clean state; a timed recording
rehearsal is still required on the instructor's machine.

All new listeners bind to loopback. Customer IDs and `X-Tadka-Client` headers in
the labs are caller supplied; there is no production authentication or authorization.
An order ID, cursor, client marker or SSE event ID is not a security credential.
Do not expose these labs publicly.

No cache, broker, distributed service fleet or browser UI is required by these
launch labs. Follow each branch's contract for measurement boundaries, cleanup
and what the experiment does not prove.

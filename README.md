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

Lessons 02, 03, 04 and 06 are board lessons and do not need branches.
Later lesson branches are not published yet.

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

For Lessons 07 and 08, install .NET 10, Docker and Node 22+, switch to the branch,
then follow its contract. Run one lesson API/database at a time because they share
ports 5080 and 5433. Stop only that lesson's Compose project before switching.
The databases are disposable. API-only restart preserves data; database teardown
resets it.

The setup-check command for the Start Here video is still planned. `main` is an
index, not a runnable application. Do not record that setup check as available.

## Evolution and limits

Networking and API contracts each start from the baseline. The SSE branch builds
on API contracts. Networking does not silently change the Order API.

Lessons 07 and 08 intentionally remove the baseline's artificial SQL sleep.
Their assertion scripts first reproduce unsafe behaviour, then verify the fix.
The new labs have been run repeatedly from clean state; a timed recording
rehearsal is still required on the instructor's machine.

All new listeners bind to loopback. Customer IDs in the lab are caller supplied;
there is no production authentication or authorization. An order ID, cursor or
SSE event ID is not a security credential. Do not expose these labs publicly.

No cache, broker, distributed service fleet or browser UI is required by these
three labs. Follow each branch's contract for measurement boundaries, cleanup
and what the experiment does not prove.

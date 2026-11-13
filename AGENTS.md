# AGENTS.md

Instructions for any AI coding assistant working in this repository (Claude Code, GitHub Copilot, Cursor, Codex and others). `CLAUDE.md` and `.github/copilot-instructions.md` point here, so there is one source of truth.

## What this repository is

Tadka is the case study for the Desi Architect System Design Masterclass. It is not a product. Each lesson that needs a lab has its own branch, and every lab follows the same loop:

**Inspect → Break → Diagnose → Evolve**

The reference implementation is ASP.NET Core and PostgreSQL. The concepts are not .NET concepts. Learners are encouraged to rebuild each lab in the language they use at work (Java, Node.js, Python, Go and others) and run the same experiment against it.

## The one rule: the experiment matters more than the code

Each lab exists to make one behaviour visible and measurable. When you build, change or port a lab:

1. **Read the lesson contract first.** `docs/lessons/lesson-NN-contract.md` defines what must stay the same in every language: endpoint, schema, settings, timer boundaries, log format and the runs. Do not change the contract to make a port easier.
2. **Keep it small.** Build only what the experiment needs. No authentication, frontend, extra endpoints, ORMs, caches, queues or frameworks the contract does not ask for. Each of those has its own later lesson.
3. **Set every setting that the experiment depends on explicitly.** Never rely on a library default for pool size, pool timeout or delays. Defaults differ between libraries and versions.
4. **Measure honestly.** Each timer must start and stop exactly where the contract says. Two separate things must never be measured with one timer.
5. **Never invent results.** Do not write expected latencies, throughput or error counts into code, comments, README or commit messages as if they were measured. Run the experiment and report what the output actually shows, including surprises.
6. **Say when you are unsure.** If you are not certain how a library behaves (for example, whether a timeout applies to waiting for a pooled connection or only to opening a new one), say so and point to the library's documentation for the version in use. Do not guess.

## Repository layout

| Path | What it is |
|---|---|
| `src/Tadka.OrderApi` | Reference implementation (ASP.NET Core, Npgsql) |
| `database/init.sql` | Schema, shared by every port |
| `docker-compose.yml` | PostgreSQL 16 on port 5433, shared by every port |
| `load-tests/` | k6 scripts, shared by every port. Never fork them per language. |
| `scripts/` | Database activity monitor and reset, shared by every port |
| `docs/lessons/` | One contract per lesson. Language-neutral. |
| `ports/<language>-<framework>/` | Learner ports, one folder each. Example: `ports/java-spring`, `ports/node-express`, `ports/python-fastapi`, `ports/go-nethttp` |

A port reuses the shared database, load tests and scripts. It only adds its own API code and a short README.

## Commands

```bash
docker compose up -d --wait                             # PostgreSQL
cd src/Tadka.OrderApi && dotnet run -c Release --launch-profile demo   # reference API on :5080
k6 run -e RATE=20 -e DURATION=10s load-tests/order-spike.js
./scripts/pg-activity.sh                                # or .ps1
./scripts/reset.sh                                      # or .ps1
docker compose down                                     # full reset
```

## Writing style for anything a learner reads

- Simple English, short sentences. Explain a term the first time it appears.
- No em dashes.
- Label every number as measured, theoretical or illustrative.
- Code comments explain why, not what.

## Do not

- Do not add infrastructure the current lesson does not need.
- Do not change `load-tests/`, `database/` or `docker-compose.yml` to suit one language.
- Do not commit generated results, logs, `bin/`, `obj/`, `node_modules/`, virtual environments or build output.
- Do not commit real credentials. The database password in `docker-compose.yml` is a local lab value only.

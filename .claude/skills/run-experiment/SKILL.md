---
name: run-experiment
description: Run the current Tadka lesson's load-test sequence against the reference API or a learner's port, capture k6 output, API log lines and PostgreSQL activity, and report what was actually measured against the lesson's theory. Use when the learner asks to run, test, benchmark, verify or compare the lab.
---

# Run the lesson experiment

## Before running

1. Read `docs/lessons/lesson-NN-contract.md` for the current branch (`git branch --show-current`). The runs, settings and theory are there.
2. Ask which implementation to run if it is not clear: the reference in `src/` or a folder in `ports/`.
3. Check the tools: `docker`, `k6`, and the build tool for that implementation. If one is missing, tell the learner how to install it and stop.
4. Start the database: `docker compose up -d --wait`. Reset it: `./scripts/reset.sh` (or `.ps1`).
5. Start the API with the hold time the run needs. Read its startup settings line and confirm pool size, pool timeout and hold time match the contract. If they do not, stop and fix that first.
6. Send one warm-up request. The first requests after a start include opening real connections.

## Running

For each run in the contract:

- Run the k6 command exactly as written. Save its output.
- While it runs, sample `./scripts/pg-activity.sh` (or `.ps1`) and save the samples.
- Save the API log lines for that run only.
- Reset the table between runs. Restart the API when the hold time changes.

Run 3 deliberately goes past the pool's capacity, so failures are expected there. Do not "fix" them during the experiment.

## Reporting

Report per run, from the saved output only:

- Requests sent (`http_reqs` count), succeeded, failed, and `dropped_iterations` if present.
- Response time from k6 (avg, median, max).
- From the API log: `db_ms` range and how `wait_ms` changed over the run.
- Active and idle client sessions from the database samples.
- For failures: the exact error text from the log.

Then compare with the theory in the contract and say plainly where they match and where they do not.

## Reading traps (say these when they apply)

- k6's `http_reqs .../s` divides by the whole test time, including waiting after arrivals stop. It is not the arrival rate. Use the count.
- `pg_stat_activity` cannot show requests waiting inside the application's pool. That is what `wait_ms` is for.
- The API log is in completion order. In an overloaded run, a request that arrived t seconds in waits about t seconds, so `wait_ms` in the log climbs more slowly than arrival time.
- These are numbers from this machine. They are not a benchmark and must not be presented as one.

## Never

- Never fill in a number that was not in the saved output.
- Never round away a surprise. If something looks wrong (for example, a response faster than the hold time), report it as unexplained.
- Never commit the results files.

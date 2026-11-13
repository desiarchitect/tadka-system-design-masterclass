# Copilot instructions

Read and follow `AGENTS.md` at the repository root. It is the single source of truth for every AI assistant in this repo.

The short version:

- Each lesson branch has a language-neutral contract in `docs/lessons/lesson-NN-contract.md`. Read it before building, porting or running a lab.
- When porting a lab to another language, put it in `ports/<language>-<framework>/` and reuse `docker-compose.yml`, `database/`, `load-tests/` and `scripts/` unchanged.
- Set every pool setting explicitly. Keep the two timers (`wait_ms`, `db_ms`) at exactly the boundaries the contract defines.
- Never write expected numbers as measured results.

---
name: port-lab
description: Rebuild the current Tadka lesson lab in another language or framework (Java, Node.js, Python, Go, Rust and others) so the same experiment runs against it. Use when the learner asks to build, port, rewrite or translate the lab or the Order API into their own stack.
---

# Port a lesson lab to another language

The goal is not a nice app in a new language. The goal is the **same experiment**, with the same measurements, so the learner can run the shared k6 script against their own code and see the same behaviour.

## Steps

1. **Find the lesson.** Run `git branch --show-current`. A branch `lesson-NN-...` maps to `docs/lessons/lesson-NN-contract.md`. Read the whole contract before writing code. If there is no contract for this branch, stop and tell the learner.
2. **Read the reference implementation** in `src/` to see how the contract is met, especially where each timer starts and stops and how connections are released.
3. **Ask only what you cannot infer.** Usually just the language and, if it matters, the framework and database driver. Prefer the most common, well-documented choice for that stack and say which one you picked.
4. **Create `ports/<language>-<framework>/`** (for example `ports/java-spring`, `ports/node-express`, `ports/python-fastapi`, `ports/go-nethttp`). Put only the API code, its build file and a short README there.
5. **Reuse the shared parts unchanged:** `docker-compose.yml`, `database/`, `load-tests/`, `scripts/`. Never copy or edit them for one language.
6. **Meet the contract exactly:** port, endpoints, the single SQL statement, explicit pool settings, the two timers, the log line format and the startup settings line.
7. **Check the driver's real behaviour.** Pool settings differ between libraries and versions. For the version you installed, confirm from its documentation:
   - the setting that limits pool size,
   - the setting that limits how long a request waits for a free pooled connection (not just how long opening a new connection may take),
   - what error the pool raises when that wait runs out.
   If you cannot confirm one of these, say so in the port's README as an open question. Do not guess.
8. **Build and start it.** Fix compile and start-up errors. Send one `POST /orders` with curl and show the log line.
9. **Hand over** to the `lab-reviewer` agent, then the `run-experiment` skill.

## The port's README

Keep it short:

- Language, framework, driver and their versions.
- How to start the API (the database and load tests are shared, so link to the root README for those).
- Where the pool settings and the two timers are in the code.
- Anything that behaves differently from the reference, and why.

## Rules

- **Smallest code that meets the contract.** No ORM, dependency injection container, auth, extra endpoints, caching or messaging unless the contract asks for it.
- **No hidden defaults.** Every pool setting the experiment depends on is written in config.
- **Release connections on every path,** using the language's structured cleanup (`try-with-resources`, `finally`, `with`, `defer`, `using`).
- **Do not cancel database work on client disconnect.**
- **Never write expected numbers as results.** Results come only from `run-experiment`.

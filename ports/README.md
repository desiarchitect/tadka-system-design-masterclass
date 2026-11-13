# Ports

One folder per port, named `<language>-<framework>`, for example `java-spring`, `node-express`, `python-fastapi`, `go-nethttp`.

Each port contains only its API code, its build file and a short README. It reuses `docker-compose.yml`, `database/`, `load-tests/` and `scripts/` from the repository root, and it must meet the lesson contract in `docs/lessons/`.

#!/usr/bin/env bash
# Empties the orders table between runs. No container restart needed.
docker exec tadka-mc-postgres psql -U tadka -d tadka -c "TRUNCATE orders RESTART IDENTITY;"

# Empties order tables between runs. Catalog seed rows remain intact. No container restart needed.
docker exec tadka-mc-postgres psql -U tadka -d tadka -c "TRUNCATE orders RESTART IDENTITY CASCADE;"

# Pane 3: PostgreSQL sessions for the tadka database, refreshed every second.
# Shows server-side sessions only. It does NOT show requests waiting inside the
# application's connection pool. That is what wait_ms in the API log is for.
$query = @"
SELECT now()::time(0) AS at,
       count(*) FILTER (WHERE state = 'active') AS active,
       count(*) FILTER (WHERE state = 'idle')   AS idle
FROM pg_stat_activity
WHERE datname = 'tadka' AND backend_type = 'client backend' AND pid <> pg_backend_pid();
"@
while ($true) {
    docker exec tadka-mc-postgres psql -U tadka -d tadka -t -A -F ' | ' -c $query
    Start-Sleep -Seconds 1
}

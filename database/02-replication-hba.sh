#!/bin/sh
set -eu
{
  echo "host replication tadka all trust"
  echo "host replication all 127.0.0.1/32 trust"
} >> "$PGDATA/pg_hba.conf"

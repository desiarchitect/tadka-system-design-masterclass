#!/usr/bin/env bash
# Tadka setup check (Start Here video).
# Checks the tools the lesson branches need and the ports they use.
# It only reads your machine. It installs and changes nothing.
# Usage: ./scripts/setup-check.sh

failed=0
warned=0

ok()   { printf '[OK]   %s\n' "$1"; }
bad()  { printf '[FAIL] %s\n' "$1"; failed=$((failed + 1)); }
warn() { printf '[WARN] %s\n' "$1"; warned=$((warned + 1)); }
has()  { command -v "$1" >/dev/null 2>&1; }

echo "Tadka setup check"
echo
echo "Required for every lab branch:"

if has git; then ok "git: $(git --version)"; else bad "git not found. Install Git."; fi

if has dotnet; then
  sdk10=$(dotnet --list-sdks 2>/dev/null | grep -E '^10\.' | head -n 1 | cut -d ' ' -f 1)
  if [ -n "$sdk10" ]; then ok ".NET SDK: $sdk10"
  else bad ".NET 10 SDK not found. Install the .NET 10 SDK."; fi
else
  bad "dotnet not found. Install the .NET 10 SDK."
fi

if has docker; then
  if docker info >/dev/null 2>&1; then ok "Docker engine: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  else bad "Docker is installed but the engine is not running. Start Docker and run this check again."; fi
  if docker compose version >/dev/null 2>&1; then ok "Docker Compose: $(docker compose version --short 2>/dev/null)"
  else bad "'docker compose' (Compose v2) not found. Update Docker Desktop or install the Compose plugin."; fi
else
  bad "docker not found. Install Docker Desktop (or Docker Engine with the Compose plugin)."
fi

echo
echo "Needed by some lessons:"

if has node; then
  node_version=$(node --version | sed 's/^v//')
  node_major=${node_version%%.*}
  if [ "$node_major" -ge 22 ] 2>/dev/null; then ok "Node.js: $node_version (runs scripts/verify-*.mjs from Lesson 07 on)"
  else warn "Node.js $node_version found. Lessons 07 and later need Node 22 or newer for scripts/verify-*.mjs."; fi
else
  warn "Node.js not found. Lessons 07 and later need Node 22 or newer for scripts/verify-*.mjs."
fi

if has k6; then ok "k6: $(k6 version)"; else warn "k6 not found. Lesson 01 uses k6 to generate load."; fi

echo
echo "Ports used by the labs (only one lesson stack runs at a time):"

check_port() {
  # A successful connect means something is already listening on the port.
  if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
    warn "port $1 is in use ($2). Stop the other lesson's stack or the program using it before you start this lab."
  else
    ok "port $1 free ($2)"
  fi
}
check_port 5080 "OrderApi"
check_port 5081 "GatewayBff (Lessons 11, 26, 27, 37, 38, 40, 45)"
check_port 5082 "second OrderApi node (Lessons 27, 37, 38, 40, 45)"
check_port 5433 "PostgreSQL primary"
check_port 5434 "PostgreSQL replica (Lessons 16 and 18)"
check_port 6379 "Redis (Lessons 23 and later)"
check_port 9092 "Kafka (Lessons 31 to 36 and 45)"

echo
if [ "$failed" -gt 0 ]; then
  echo "$failed required check(s) failed. Fix them, then run this check again."
  exit 1
fi
if [ "$warned" -gt 0 ]; then
  echo "Required tools are ready. $warned warning(s) above matter only for the lessons they name."
else
  echo "Everything is ready. Switch to a lesson branch and open its contract in docs/lessons/."
fi
exit 0

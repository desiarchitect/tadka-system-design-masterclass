# Tadka setup check (Start Here video).
# Checks the tools the lesson branches need and the ports they use.
# It only reads your machine. It installs and changes nothing.
# Usage: ./scripts/setup-check.ps1   (Windows PowerShell 5.1 or PowerShell 7+)

$ErrorActionPreference = 'Continue'
$script:failed = 0
$script:warned = 0

function Write-Ok($msg)   { Write-Host "[OK]   $msg" -ForegroundColor Green }
function Write-Bad($msg)  { Write-Host "[FAIL] $msg" -ForegroundColor Red; $script:failed++ }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow; $script:warned++ }

function Test-Command($name) { [bool](Get-Command $name -ErrorAction SilentlyContinue) }

Write-Host "Tadka setup check"
Write-Host ""
Write-Host "Required for every lab branch:"

if (Test-Command git) { Write-Ok ("git: " + (git --version)) } else { Write-Bad "git not found. Install Git." }

if (Test-Command dotnet) {
    $sdks = @(dotnet --list-sdks 2>$null)
    $sdk10 = $sdks | Where-Object { $_ -match '^10\.' } | Select-Object -First 1
    if ($sdk10) { Write-Ok ".NET SDK: $($sdk10.Split(' ')[0])" }
    else { Write-Bad ".NET 10 SDK not found (installed: $((($sdks | ForEach-Object { $_.Split(' ')[0] }) -join ', '))). Install the .NET 10 SDK." }
} else { Write-Bad "dotnet not found. Install the .NET 10 SDK." }

if (Test-Command docker) {
    docker info *> $null
    if ($LASTEXITCODE -eq 0) { Write-Ok ("Docker engine: " + (docker version --format '{{.Server.Version}}' 2>$null)) }
    else { Write-Bad "Docker is installed but the engine is not running. Start Docker Desktop and run this check again." }
    docker compose version *> $null
    if ($LASTEXITCODE -eq 0) { Write-Ok ("Docker Compose: " + (docker compose version --short 2>$null)) }
    else { Write-Bad "'docker compose' (Compose v2) not found. Update Docker Desktop or install the Compose plugin." }
} else { Write-Bad "docker not found. Install Docker Desktop (or Docker Engine with the Compose plugin)." }

Write-Host ""
Write-Host "Needed by some lessons:"

if (Test-Command node) {
    $nodeVersion = (node --version).TrimStart('v')
    $nodeMajor = [int]($nodeVersion.Split('.')[0])
    if ($nodeMajor -ge 22) { Write-Ok "Node.js: $nodeVersion (runs scripts/verify-*.mjs from Lesson 07 on)" }
    else { Write-Warn "Node.js $nodeVersion found. Lessons 07 and later need Node 22 or newer for scripts/verify-*.mjs." }
} else { Write-Warn "Node.js not found. Lessons 07 and later need Node 22 or newer for scripts/verify-*.mjs." }

if (Test-Command k6) { Write-Ok ("k6: " + (k6 version)) }
else { Write-Warn "k6 not found. Lesson 01 uses k6 to generate load." }

Write-Host ""
Write-Host "Ports used by the labs (only one lesson stack runs at a time):"

$ports = [ordered]@{
    5080 = 'OrderApi'
    5081 = 'GatewayBff (Lessons 11, 26, 27, 37, 38, 40, 45)'
    5082 = 'second OrderApi node (Lessons 27, 37, 38, 40, 45)'
    5433 = 'PostgreSQL primary'
    5434 = 'PostgreSQL replica (Lessons 16 and 18)'
    6379 = 'Redis (Lessons 23 and later)'
    9092 = 'Kafka (Lessons 31 to 36 and 45)'
}
foreach ($entry in $ports.GetEnumerator()) {
    $port = $entry.Key
    $label = $entry.Value
    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
        $listener.Start()
        Write-Ok "port $port free ($label)"
    } catch {
        Write-Warn "port $port is in use ($label). Stop the other lesson's stack or the program using it before you start this lab."
    } finally {
        if ($listener) { $listener.Stop() }
    }
}

Write-Host ""
if ($script:failed -gt 0) {
    Write-Host "$($script:failed) required check(s) failed. Fix them, then run this check again." -ForegroundColor Red
    exit 1
}
if ($script:warned -gt 0) {
    Write-Host "Required tools are ready. $($script:warned) warning(s) above matter only for the lessons they name."
} else {
    Write-Host "Everything is ready. Switch to a lesson branch and open its contract in docs/lessons/."
}
exit 0

param(
    [string]$GdbName = "KONYA.gdb",
    [switch]$ImportAllGdbs,
    [switch]$AppendImport,
    [ValidateSet("all", "core")]
    [string]$ImportMode = "all",
    [double]$Lat = 37.8715,
    [double]$Lon = 32.4846,
    [double]$DoorRadiusM = 15,
    [double]$BuildingRadiusM = 40,
    [double]$RoadRadiusM = 90,
    [ValidateSet("geodesic", "planar")]
    [string]$Metric = "geodesic",
    [switch]$SkipInspect,
    [switch]$SkipImport
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$nativeErrPrefVar = Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue
if ($null -ne $nativeErrPrefVar) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function Invoke-External {
    param(
        [string]$Command,
        [string]$FailMessage
    )

    cmd /c "$Command 2>&1"
    if ($LASTEXITCODE -ne 0) {
        throw $FailMessage
    }
}

function Run-Step {
    param(
        [string]$Title,
        [scriptblock]$Script
    )

    Write-Host "`n=== $Title ===" -ForegroundColor Cyan
    & $Script
    Write-Host "OK: $Title" -ForegroundColor Green
}

function Assert-DockerReady {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Docker komutu bulunamadi. Docker Desktop'i kurup PATH'e ekle."
    }

    Invoke-External -Command "docker info >nul" -FailMessage "Docker daemon calismiyor. Docker Desktop'i baslat ve tekrar dene."
}

function Wait-ApiHealth {
    param(
        [string]$Url = "http://localhost:8000/health",
        [int]$MaxAttempts = 20,
        [int]$DelaySeconds = 3
    )

    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 10
            if ($resp.status -eq "ok") {
                return $resp
            }
        }
        catch {
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "API health kontrolu zaman asimina ugradi: $Url"
}

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $projectRoot

$gdbPaths = @()
if ($ImportAllGdbs) {
    $rawDir = Join-Path $projectRoot "data/raw_gdb"
    $gdbDirs = Get-ChildItem -Path $rawDir -Directory -Filter "*.gdb" -ErrorAction SilentlyContinue
    if (-not $gdbDirs -or $gdbDirs.Count -eq 0) {
        throw "ImportAllGdbs acik ama data/raw_gdb altinda .gdb klasoru bulunamadi."
    }
    $gdbPaths = @($gdbDirs | ForEach-Object { $_.FullName })
}
else {
    $localGdbPath = Join-Path $projectRoot ("data/raw_gdb/" + $GdbName)
    if (-not (Test-Path $localGdbPath)) {
        throw "GDB bulunamadi: $localGdbPath"
    }
    $gdbPaths = @($localGdbPath)
}

Run-Step "Docker hazirlik kontrolu" {
    Assert-DockerReady
}

Run-Step "Servisleri build + up" {
    Invoke-External -Command "set DOCKER_BUILDKIT=0&& set COMPOSE_DOCKER_CLI_BUILD=0&& docker compose build api etl" -FailMessage "docker compose build basarisiz"
    Invoke-External -Command "docker compose up -d" -FailMessage "docker compose up basarisiz"
}

if (-not $SkipInspect) {
    Run-Step "GDB katman analizi" {
        foreach ($gdbPath in $gdbPaths) {
            $gdbNameLocal = Split-Path $gdbPath -Leaf
            $containerGdbPath = "/data/raw_gdb/$gdbNameLocal"
            Write-Host "Inspect: $gdbNameLocal" -ForegroundColor DarkCyan
            Invoke-External -Command "docker compose run --rm etl -lc ""bash /etl/scripts/inspect_gdb.sh $containerGdbPath""" -FailMessage "inspect adimi basarisiz ($gdbNameLocal)"
        }
    }
}

if (-not $SkipImport) {
    Run-Step "GDB -> raw_maks import" {
        $isFirst = $true
        foreach ($gdbPath in $gdbPaths) {
            $gdbNameLocal = Split-Path $gdbPath -Leaf
            $containerGdbPath = "/data/raw_gdb/$gdbNameLocal"
            $importBehavior = if ($AppendImport -or (-not $isFirst)) { "append" } else { "overwrite" }
            Write-Host "Import: $gdbNameLocal (mode=$ImportMode, behavior=$importBehavior)" -ForegroundColor DarkCyan
            Invoke-External -Command "docker compose run --rm etl -lc ""IMPORT_MODE=$ImportMode IMPORT_BEHAVIOR=$importBehavior bash /etl/scripts/import_gdb_to_raw.sh $containerGdbPath""" -FailMessage "import adimi basarisiz ($gdbNameLocal)"
            $isFirst = $false
        }
    }

    Run-Step "Raw profil JSON olusturma" {
        Invoke-External -Command "docker compose run --rm etl -lc ""python -u /etl/scripts/profile_raw.py""" -FailMessage "raw profil adimi basarisiz"
    }
}

Run-Step "API health kontrolu" {
    $health = Wait-ApiHealth
    if ($health.status -ne "ok") { throw "API health ok donmedi" }
}

Run-Step "Reverse geocode smoke test" {
    $url = "http://localhost:8000/reverse-geocode?lat=$Lat&lon=$Lon&door_radius_m=$DoorRadiusM&building_radius_m=$BuildingRadiusM&road_radius_m=$RoadRadiusM&metric=$Metric"
    $wc = New-Object System.Net.WebClient
    $bytes = $wc.DownloadData($url)
    $json = [System.Text.Encoding]::UTF8.GetString($bytes)
    $resp = $json | ConvertFrom-Json
    $resp | ConvertTo-Json -Depth 6
}

Write-Host "`nPipeline tamamlandi." -ForegroundColor Green

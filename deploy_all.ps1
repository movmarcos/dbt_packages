# Create Python venv (if missing), install deps, run dbt deploy to Snowflake.
# Loops over one or more dbt projects, calling deploy.py for each.
#
# Usage:
#   .\deploy_all.ps1 -Target test -Projects dbt_package
#   .\deploy_all.ps1 -Target test -Projects dbt_package,other_project
#   .\deploy_all.ps1 -Target release -Projects dbt_package,other_project -Suffix 25_04
#   .\deploy_all.ps1 -Target prod -Projects dbt_package -DbtArgs "run --select tag:daily"
#   .\deploy_all.ps1 -Target test -Projects dbt_package -SkipPull   # CI: source already synced
#
# Project resolution:
#   Each name in -Projects is resolved as a folder under -ProjectsRoot (defaults
#   to the directory containing this script). A folder must contain dbt_project.yml.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dev', 'test', 'release', 'prod')]
    [string]$Target,

    [Parameter(Mandatory = $true)]
    [string[]]$Projects,

    [string]$ProjectsRoot = $PSScriptRoot,

    [string]$Suffix,

    [string]$DbtArgs = 'build',

    [switch]$UploadOnly,
    [switch]$ExecuteOnly,
    [switch]$SkipPull
)

$ErrorActionPreference = 'Stop'

$ScriptDir        = $PSScriptRoot
$VenvDir          = Join-Path $ScriptDir ".venv"
$RequirementsFile = Join-Path $ScriptDir "requirements.txt"
$DeployScript     = Join-Path $ScriptDir "deploy.py"

# System Python used to create the venv. Adjust for your TeamCity agent.
$SystemPython = "C:/Users/n319464/AppData/Local/Programs/Python/Python313/python.exe"

# [1/4] Pull latest changes
if (-not $SkipPull) {
    Write-Host "[1/4] Pulling latest changes from git..." -ForegroundColor Cyan
    git pull
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Git pull failed. Aborting deployment." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[1/4] Skipping git pull (-SkipPull)" -ForegroundColor Yellow
}

# [2/4] Create virtual environment if missing
Write-Host "[2/4] Preparing virtual environment..." -ForegroundColor Cyan
if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating venv at $VenvDir" -ForegroundColor Gray
    & $SystemPython -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to create virtual environment." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Using existing venv at $VenvDir" -ForegroundColor Gray
}

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Host "Venv python not found at $VenvPython" -ForegroundColor Red
    exit 1
}

# [3/4] Install / upgrade dependencies
Write-Host "[3/4] Installing dependencies..." -ForegroundColor Cyan
& $VenvPython -m pip install --upgrade pip | Out-Null
if (Test-Path $RequirementsFile) {
    & $VenvPython -m pip install -r $RequirementsFile
} else {
    & $VenvPython -m pip install mufg_snowflakeconn snowflake-snowpark-python
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "Dependency install failed." -ForegroundColor Red
    exit 1
}

# [4/4] Loop projects → deploy.py per project
Write-Host "[4/4] Deploying $($Projects.Count) project(s) to target=$Target (suffix=$Suffix)..." -ForegroundColor Cyan

$failures = @()

foreach ($project in $Projects) {
    $projectDir = Join-Path $ProjectsRoot $project
    $dbtProjectYml = Join-Path $projectDir "dbt_project.yml"

    Write-Host ""
    Write-Host ("─" * 64) -ForegroundColor DarkGray
    Write-Host "▶ Project: $project  ($projectDir)" -ForegroundColor Cyan
    Write-Host ("─" * 64) -ForegroundColor DarkGray

    if (-not (Test-Path $dbtProjectYml)) {
        Write-Host "  ❌ dbt_project.yml not found — skipping." -ForegroundColor Red
        $failures += $project
        continue
    }

    $deployArgs = @(
        '--target',       $Target,
        '--project-name', $project,
        '--project-dir',  $projectDir,
        '--dbt-args',     $DbtArgs
    )
    if ($Suffix)      { $deployArgs += @('--suffix', $Suffix) }
    if ($UploadOnly)  { $deployArgs += '--upload-only' }
    if ($ExecuteOnly) { $deployArgs += '--execute-only' }

    & $VenvPython $DeployScript @deployArgs

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ❌ Deployment failed for $project." -ForegroundColor Red
        $failures += $project
    }
}

Write-Host ""
Write-Host ("=" * 64) -ForegroundColor DarkGray
if ($failures.Count -eq 0) {
    Write-Host "All $($Projects.Count) project(s) deployed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Deployment finished with errors in: $($failures -join ', ')" -ForegroundColor Red
    exit 1
}

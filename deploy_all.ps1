# ============================================================================
#  deploy_all.ps1 -- dbt -> Snowflake deployment wrapper
# ============================================================================
#  Pipeline overview:
#
#   [STEP 1/4] git pull                       (skippable with -SkipPull)
#   [STEP 2/4] Create / reuse Python venv
#   [STEP 3/4] Install Python dependencies    (requirements.txt)
#   [STEP 4/4] Loop -Projects, call deploy.py once per project:
#                   * PHASE 1 -- upload project files to Snowflake stage
#                   * PHASE 2 -- register the DBT PROJECT (CREATE OR REPLACE)
#                   * PHASE 3 -- EXECUTE DBT PROJECT ... ARGS='<dbt_args>'
#
#  Project resolution:
#    Each name in -Projects is resolved as a folder under -ProjectsRoot
#    (defaults to the directory containing this script). The folder must
#    contain dbt_project.yml.
#
#  Required: mufg_snowflakeconn, snowflake-snowpark-python (via requirements.txt).
#  See the EXAMPLES block at the bottom of this file for common invocations.
# ============================================================================

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dvlp', 'test', 'rlse', 'prod')]
    [string]$Target,

    [Parameter(Mandatory = $true)]
    [string[]]$Projects,

    [string]$ProjectsRoot = $PSScriptRoot,

    [string]$DbType = 'RAPTOR',

    [string]$Suffix,

    [string]$DbtArgs = 'build',

    [switch]$UploadOnly,
    [switch]$ExecuteOnly,
    [switch]$SkipPull
)

# -Suffix is required for every target except prod.
if ($Target -ne 'prod' -and [string]::IsNullOrWhiteSpace($Suffix)) {
    Write-Host "-Suffix is required for target '$Target' (only 'prod' may omit it)." -ForegroundColor Red
    exit 2
}

$ErrorActionPreference = 'Stop'

# Layout -- this script and its siblings (deploy.py, requirements.txt, .venv)
# all live in $ScriptDir. Project folders live under $ProjectsRoot.
$ScriptDir        = $PSScriptRoot
$VenvDir          = Join-Path $ScriptDir ".venv"
$RequirementsFile = Join-Path $ScriptDir "requirements.txt"
$DeployScript     = Join-Path $ScriptDir "deploy.py"

# Separator strings (built once, ASCII-only to avoid encoding issues).
$SepHeavy = ("=" * 72)
$SepLight = ("-" * 72)

# System Python used to create the venv. Adjust for your TeamCity agent.
$SystemPython = "C:/Users/n319464/AppData/Local/Programs/Python/Python313/python.exe"

# ---- Helper: boxed step header -------------------------------------------
function Write-Step {
    param(
        [int]$Num,
        [int]$Total,
        [string]$Title,
        [string]$Color = 'Cyan'
    )
    $ts = (Get-Date).ToString('HH:mm:ss')
    Write-Host ""
    Write-Host $SepLight -ForegroundColor DarkGray
    Write-Host "  [STEP $Num/$Total] $ts  --  $Title" -ForegroundColor $Color
    Write-Host $SepLight -ForegroundColor DarkGray
}

# ---- Run banner (shown once at the top) ----------------------------------
$runStart = Get-Date
Write-Host $SepHeavy -ForegroundColor DarkGray
Write-Host "  dbt -> Snowflake Deployment" -ForegroundColor Cyan
Write-Host $SepHeavy -ForegroundColor DarkGray
Write-Host ("  Started      : " + $runStart.ToString('yyyy-MM-dd HH:mm:ss'))
Write-Host ("  Target       : $Target")
Write-Host ("  DB type      : $DbType")
Write-Host ("  Suffix       : " + $(if ($Suffix) { $Suffix } else { '(none - prod)' }))
Write-Host ("  Projects     : " + ($Projects -join ', '))
Write-Host ("  ProjectsRoot : $ProjectsRoot")
Write-Host ("  dbt args     : $DbtArgs")
$modeParts = @()
if ($UploadOnly)  { $modeParts += 'upload-only' }
if ($ExecuteOnly) { $modeParts += 'execute-only' }
if ($SkipPull)    { $modeParts += 'skip-pull' }
if ($modeParts.Count -gt 0) {
    Write-Host ("  Mode flags   : " + ($modeParts -join ', '))
}
Write-Host $SepHeavy -ForegroundColor DarkGray

# ---- [STEP 1/4] git pull -------------------------------------------------
if (-not $SkipPull) {
    Write-Step 1 4 "Pull latest changes from git"
    git pull
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [FAIL] Git pull failed. Aborting deployment." -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Source up to date." -ForegroundColor Green
} else {
    Write-Step 1 4 "Pull latest changes from git  (SKIPPED via -SkipPull)" 'Yellow'
}

# ---- [STEP 2/4] venv -----------------------------------------------------
Write-Step 2 4 "Prepare Python virtual environment"
if (-not (Test-Path $VenvDir)) {
    Write-Host "  Creating venv at $VenvDir" -ForegroundColor Gray
    & $SystemPython -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [FAIL] Failed to create virtual environment." -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Venv created." -ForegroundColor Green
} else {
    Write-Host "  Using existing venv at $VenvDir" -ForegroundColor Gray
    Write-Host "  [OK] Venv ready." -ForegroundColor Green
}

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Host "  [FAIL] Venv python not found at $VenvPython" -ForegroundColor Red
    exit 1
}

# ---- [STEP 3/4] pip install ----------------------------------------------
Write-Step 3 4 "Install Python dependencies"
& $VenvPython -m pip install --upgrade pip | Out-Null
if (Test-Path $RequirementsFile) {
    Write-Host "  Installing from $RequirementsFile" -ForegroundColor Gray
    & $VenvPython -m pip install -r $RequirementsFile
} else {
    Write-Host "  No requirements.txt; installing baseline packages." -ForegroundColor Gray
    & $VenvPython -m pip install mufg_snowflakeconn snowflake-snowpark-python
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [FAIL] Dependency install failed." -ForegroundColor Red
    exit 1
}
Write-Host "  [OK] Dependencies ready." -ForegroundColor Green

# ---- [STEP 4/4] loop and deploy each project -----------------------------
Write-Step 4 4 "Deploy $($Projects.Count) project(s) to target=$Target"

$succeeded = @()
$failed    = @()

for ($i = 0; $i -lt $Projects.Count; $i++) {
    $project       = $Projects[$i]
    $projectDir    = Join-Path $ProjectsRoot $project
    $dbtProjectYml = Join-Path $projectDir "dbt_project.yml"
    $projNum       = $i + 1
    $projTotal     = $Projects.Count

    Write-Host ""
    Write-Host ("+" + ("-" * 70)) -ForegroundColor DarkCyan
    Write-Host "|  Project [$projNum/$projTotal]: $project" -ForegroundColor Cyan
    Write-Host "|  Folder         : $projectDir" -ForegroundColor Gray
    Write-Host ("+" + ("-" * 70)) -ForegroundColor DarkCyan

    if (-not (Test-Path $dbtProjectYml)) {
        Write-Host "  [FAIL] dbt_project.yml not found at $dbtProjectYml - skipping." -ForegroundColor Red
        $failed += $project
        continue
    }

    # Assemble the arg list for deploy.py
    $deployArgs = @(
        '--target',       $Target,
        '--project-name', $project,
        '--project-dir',  $projectDir,
        '--db-type',      $DbType,
        '--dbt-args',     $DbtArgs
    )
    if ($Suffix)      { $deployArgs += @('--suffix', $Suffix) }
    if ($UploadOnly)  { $deployArgs += '--upload-only' }
    if ($ExecuteOnly) { $deployArgs += '--execute-only' }

    & $VenvPython $DeployScript @deployArgs

    if ($LASTEXITCODE -eq 0) {
        $succeeded += $project
    } else {
        Write-Host "  [FAIL] Deployment failed for $project (deploy.py exit $LASTEXITCODE)." -ForegroundColor Red
        $failed += $project
    }
}

# ---- Final summary -------------------------------------------------------
$runEnd  = Get-Date
$elapsed = $runEnd - $runStart
$elapsedStr = "{0:D2}:{1:D2}:{2:D2}" -f $elapsed.Hours, $elapsed.Minutes, $elapsed.Seconds

Write-Host ""
Write-Host $SepHeavy -ForegroundColor DarkGray
Write-Host "  Deployment Summary" -ForegroundColor Cyan
Write-Host $SepHeavy -ForegroundColor DarkGray
Write-Host ("  Target       : $Target")
Write-Host ("  Projects     : " + $Projects.Count)
Write-Host ("  Succeeded    : " + $succeeded.Count) -ForegroundColor Green
Write-Host ("  Failed       : " + $failed.Count) -ForegroundColor $(if ($failed.Count) { 'Red' } else { 'Green' })
if ($succeeded.Count) { Write-Host ("    [OK]   " + ($succeeded -join ', ')) -ForegroundColor Green }
if ($failed.Count)    { Write-Host ("    [FAIL] " + ($failed    -join ', ')) -ForegroundColor Red }
Write-Host ("  Elapsed      : $elapsedStr")
Write-Host ("  Finished     : " + $runEnd.ToString('yyyy-MM-dd HH:mm:ss'))
Write-Host $SepHeavy -ForegroundColor DarkGray

if ($failed.Count -eq 0) {
    Write-Host "All $($Projects.Count) project(s) deployed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Deployment finished with errors." -ForegroundColor Red
    exit 1
}

# ============================================================================
#  EXAMPLES -- copy/paste and adjust
# ============================================================================
#
# Single project, dvlp environment, release 25_04
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04
#
# Two projects in one run (loops through them sequentially)
#   .\deploy_all.ps1 -Target test -Projects dbt_package,other_project -Suffix 25_04
#
# Release (UAT) deployment
#   .\deploy_all.ps1 -Target rlse -Projects dbt_package -Suffix 25_04
#
# Production -- no suffix needed (only prod may omit it)
#   .\deploy_all.ps1 -Target prod -Projects dbt_package
#
# Custom dbt command instead of 'build' (default)
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04 `
#                    -DbtArgs "run --select tag:daily"
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04 -DbtArgs "test"
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04 -DbtArgs "seed"
#
# Override the DB type component (default: RAPTOR -> e.g. DVLP_CREDIT_*)
#   .\deploy_all.ps1 -Target test -Projects dbt_package -Suffix 25_04 -DbType CREDIT
#
# Upload + register only (no dbt execution)
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04 -UploadOnly
#
# Skip upload, just execute dbt against the already-staged project
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package -Suffix 25_04 -ExecuteOnly
#
# CI / TeamCity -- workspace already synced, skip git pull
#   .\deploy_all.ps1 -Target test -Projects dbt_package -Suffix 25_04 -SkipPull
#
# Projects living in a different folder than this script
#   .\deploy_all.ps1 -Target dvlp -Projects dbt_package,other_project `
#                    -ProjectsRoot C:\repos\mufg-dbt -Suffix 25_04
# ============================================================================

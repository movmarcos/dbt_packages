<#
.SYNOPSIS
    Main orchestration script for Raptor file upload and listing workflow

.DESCRIPTION
    This script orchestrates the complete workflow:
    1. Get list of files to upload from Snowflake and save to temp folder
    2. Upload files to Azure Data Lake (placeholder - to be implemented)
    3. List files from Azure Data Lake and save to Snowflake
    
    Uses certificate-based service principal authentication.
    Parameters are read from EnvParameters folder based on Environment.

.PARAMETER Environment
    Environment name (dvlp, test, rlse, prod)

.PARAMETER SnowflakeDB
    Full Snowflake database name (e.g., dvlp_RAPTOR_MM_CHECK_RAVEN, PROD_RAPTOR).

.PARAMETER COB
    Close of Business date in format YYYYMMDD

.PARAMETER StageFilter
    Stage name filter pattern (e.g., 'RAPTORDATA'). Default: 'RAPTORDATA'

.PARAMETER NumberOfCobs
    Number of COBs to process going backwards from the specified COB date.
    1 = only the COB date specified
    2 = the COB date and the previous one
    Default: 1

.PARAMETER Steps
    Which steps to run (comma-separated): 1, 2, 3
    Step 1: Get file list from Snowflake
    Step 2: Upload files to Azure
    Step 3: List Azure files and save to Snowflake
    Default: "1,2,3" (all steps)

.PARAMETER AuthMethod
    Authentication method for Azure: 'Certificate' or 'CurrentUser'
    Certificate: Uses Service Principal with certificate (default)
    CurrentUser: Uses current logged-in user credentials
    Default: "Certificate"

.EXAMPLE
    .\Main_Upload_List_Files.ps1 -Environment "dvlp" -SnowflakeDB "dvlp_RAPTOR_MM_CHECK_RAVEN" -COB "20251027" -NumberOfCobs 1
    # Processes only 20251027, runs all steps

.EXAMPLE
    .\Main_Upload_List_Files.ps1 -Environment "dvlp" -SnowflakeDB "dvlp_RAPTOR_MM_CHECK_RAVEN" -COB "20251027" -NumberOfCobs 2 -Steps "1,3"
    # Processes 20251024 and 20251027, runs only steps 1 and 3 (skips upload)

.EXAMPLE
    .\Main_Upload_List_Files.ps1 -Environment "dvlp" -SnowflakeDB "dvlp_RAPTOR_MM_CHECK_RAVEN" -COB "20251027" -Steps "3" -AuthMethod "CurrentUser"
    # Runs only step 3 (list and save) using current user authentication

.EXAMPLE
    .\Main_Upload_List_Files.ps1 -Environment "prod" -SnowflakeDB "PROD_RAPTOR" -COB "20251031" -NumberOfCobs 1
    # Production environment with full database name
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("dvlp", "test", "rlse", "prod")]
    [string]$Environment,
    
    [Parameter(Mandatory=$true)]
    [string]$SnowflakeDB,
    
    [Parameter(Mandatory=$true)]
    [string]$COB,
    
    [Parameter(Mandatory=$false)]
    [string]$StageFilter = "RAPTORDATA",
    
    [Parameter(Mandatory=$false)]
    [int]$NumberOfCobs = 1,
    
    [Parameter(Mandatory=$false)]
    [string]$Steps = "1,2,3",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("Certificate", "CurrentUser")]
    [string]$AuthMethod = "Certificate",

    [Parameter(Mandatory=$false)]
    [string]$OverwriteOption = "SYNC"
)

$env:PSModulePath = "$PSScriptRoot\modules;C:\Windows\system32\WindowsPowerShell\v1.0\Modules"

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "`n================================================================================" -ForegroundColor Cyan
Write-Host "Raptor File Upload & List Workflow" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host

# Parse steps to run
$StepsToRun = $Steps -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^\d+$' }
$RunStep1 = $StepsToRun -contains "1"
$RunStep2 = $StepsToRun -contains "2"
$RunStep3 = $StepsToRun -contains "3"

Write-Host "Steps to run: $($StepsToRun -join ', ')" -ForegroundColor White
Write-Host

# Validate COB format
if ($COB -notmatch '^\d{8}$') {
    Write-Host "COB must be in format YYYYMMDD (e.g., 20251024)" -ForegroundColor Red
    Write-Host "`n================================================================================" -ForegroundColor Cyan
    exit 1
}

# Calculate COB range based on NumberOfCobs
$CobDate = [datetime]::ParseExact($COB, "yyyyMMdd", $null)
$CobList = @()

if ($NumberOfCobs -eq 1) {
    # Single COB - just use the provided date
    $CobList += $COB
}
else {
    # Multiple COBs - skip weekends (Saturday and Sunday)
    $CurrentDate = $CobDate
    $CobsCollected = 0
    
    while ($CobsCollected -lt $NumberOfCobs) {
        $DayOfWeek = $CurrentDate.DayOfWeek
        
        # Only add if it's a weekday (Monday=1 to Friday=5)
        if ($DayOfWeek -ne [System.DayOfWeek]::Saturday -and $DayOfWeek -ne [System.DayOfWeek]::Sunday) {
            $CobList += $CurrentDate.ToString("yyyyMMdd")
            $CobsCollected++
        }
        
        # Move to previous day for next iteration
        if ($CobsCollected -lt $NumberOfCobs) {
            $CurrentDate = $CurrentDate.AddDays(-1)
        }
    }
    
    # Reverse the list so oldest COB is first
    [array]::Reverse($CobList)
}

$CobIdStart = [int]$CobList[0]
$CobIdEnd = [int]$CobList[-1]

Write-Host "`n--- COB Calculation ---" -ForegroundColor Cyan
Write-Host "Requested COB    : $COB" -ForegroundColor White
Write-Host "Number of COBs   : $NumberOfCobs" -ForegroundColor White
Write-Host "COB Range        : $CobIdStart to $CobIdEnd" -ForegroundColor White
Write-Host

# Read environment parameters
$EnvFolder = switch ($Environment.ToLower()) {
    "dvlp" { "Dvlp" }
    "test" { "Test" }
    "rlse" { "Rlse" }
    "prod" { "Prod" }
}

$ParameterPath = Join-Path $ScriptDir "EnvParameters\$EnvFolder\AdlsParameters.json"

if (-not (Test-Path $ParameterPath)) {
    Write-Host "Parameter file not found: $ParameterPath" -ForegroundColor Red
    Write-Host "`n================================================================================" -ForegroundColor Cyan
    exit 1
}

try {
    $ParamsFile = Get-Content -Path $ParameterPath -Raw | ConvertFrom-Json
    $Params = $ParamsFile.Configs.Params
    
    $TenantId = $Params.TenantID.Trim()
    $ContainerName = $Params.ContainerName.Trim()
    $SubscriptionId = $Params.SubscriptionID.Trim()
    
    # Choose credentials based on StageFilter
    # QUICPLUS stages use regular credentials (has permissions on mufgeunadldvlp)
    # Other stages use Build credentials (has permissions on euntboriskraptordvlp)
    if ($StageFilter -like "*QUICPLUS*") {
        Write-Host "QUICPLUS stage detected - using QUICPLUS-specific settings" -ForegroundColor White
        $ServicePrincipalId = $Params.ServicePrincipalId.Trim()
        $CertName = $Params.CertName.Trim()
        
        # Use QUICPLUS-specific storage account if available
        if ($Params.StorageAccountName_quicplus) {
            $StorageAccountName = $Params.StorageAccountName_quicplus.Trim()
            Write-Host "  Using QUICPLUS storage account: $StorageAccountName" -ForegroundColor Cyan
        }
        else {
            $StorageAccountName = $Params.StorageAccountName.Trim()
        }
    }
    elseif ($Params.ServicePrincipalId -and $Params.CertName) {
        Write-Host "Using Service Principal" -ForegroundColor White
        $ServicePrincipalId = $Params.ServicePrincipalId.Trim()
        $CertName = $Params.CertName.Trim()
        $StorageAccountName = $Params.StorageAccountName.Trim()
    }
    elseif ($Params.Build_ServicePrincipalId -and $Params.Build_CertName) {
        Write-Host "Using Build Service Principal" -ForegroundColor White
        $ServicePrincipalId = $Params.Build_ServicePrincipalId.Trim()
        $CertName = $Params.Build_CertName.Trim()
        $StorageAccountName = $Params.StorageAccountName.Trim()
    }
    else {
        $ServicePrincipalId = $Params.ServicePrincipalId.Trim()
        $CertName = $Params.CertName.Trim()
        $StorageAccountName = $Params.StorageAccountName.Trim()
    }
    
    # Container name is the same for all stages (not used - determined from Snowflake stage)
    # Removed: $ContainerName = $Params.ContainerName.Trim()
}
catch {
    Write-Host "Failed to read parameter file" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "`n================================================================================" -ForegroundColor Cyan
    exit 1
}

# Display parameters
Write-Host "`n--- Configuration ---" -ForegroundColor Cyan
Write-Host "Environment      : $Environment" -ForegroundColor White
Write-Host "Database         : $SnowflakeDB" -ForegroundColor White
Write-Host "Storage Account  : $StorageAccountName" -ForegroundColor White
Write-Host "Stage Filter     : $StageFilter" -ForegroundColor White
Write-Host

# Find Python executable

$PythonExe = $null
$VenvPath = Join-Path $ScriptDir ".venv"

# Check if virtual environment exists
if (-not (Test-Path (Join-Path $VenvPath "Scripts\python.exe"))) {
    Write-Host "Virtual environment not found. Creating..." -ForegroundColor White
    
    # Find base Python installation
    $BasePython = $null
    if (Get-Command python -ErrorAction SilentlyContinue) {
        $BasePython = (Get-Command python).Source
    }
    elseif (Get-Command py -ErrorAction SilentlyContinue) {
        $BasePython = "py"
    }
    else {
        Write-Host "Python not found! Please install Python first." -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit 1
    }
    
    # Save current location and switch to script directory
    Push-Location $ScriptDir
    
    try {
        & $BasePython -m venv $VenvPath
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Failed to create virtual environment" -ForegroundColor Red
            Pop-Location
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit 1
        }
        
        Write-Host "Virtual environment created successfully" -ForegroundColor Green
        
        # Install packages from requirements.txt
        $RequirementsPath = Join-Path $ScriptDir "requirements.txt"
        if (Test-Path $RequirementsPath) {
            Write-Host "Installing packages from requirements.txt..." -ForegroundColor White
            $VenvPythonExe = Join-Path $VenvPath "Scripts\python.exe"
            
            # Upgrade pip first using python -m pip (avoids file lock issues)
            Write-Host "  Upgrading pip..." -ForegroundColor Gray
            $null = & $VenvPythonExe -m pip install --upgrade pip --quiet 2>&1
            
            # Install from requirements.txt
            Write-Host "  Installing dependencies..." -ForegroundColor Gray
            
            # Temporarily disable error action preference to allow pip warnings
            $PrevErrorPref = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            
            try {
                $InstallOutput = & $VenvPythonExe -m pip install -r $RequirementsPath 2>&1 | ForEach-Object { $_.ToString() }
            }
            finally {
                $ErrorActionPreference = $PrevErrorPref
            }
            
            # Convert output to string for analysis
            $OutputText = $InstallOutput -join "`n"
            
            # Check for actual installation failures
            $HasRealError = $OutputText -match "ERROR:.*failed to build" -or 
                           $OutputText -match "Could not install packages" -or
                           $OutputText -match "ERROR:.*No matching distribution"
            
            # Check for successful installation
            $HasSuccess = $OutputText -match "Successfully installed"
            
            if ($HasRealError) {
                Write-Host "    ERROR: Package installation failed!" -ForegroundColor Red
                $InstallOutput | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
                Pop-Location
                Write-Host "`n================================================================================" -ForegroundColor Cyan
                exit 1
            }
            elseif ($HasSuccess) {
                Write-Host "    All packages installed successfully" -ForegroundColor Green
                # Show dependency warnings if any (but don't fail)
                if ($OutputText -match "dependency conflicts") {
                    Write-Host "    Note: Some dependency version conflicts detected (non-fatal)" -ForegroundColor Yellow
                }
            }
            else {
                # Uncertain - show output
                $InstallOutput | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
        }
        else {
            Write-Host "requirements.txt not found at: $RequirementsPath" -ForegroundColor Yellow
        }
    }
    finally {
        # Restore original location
        Pop-Location
    }
}  # End of: if (-not (Test-Path (Join-Path $VenvPath "Scripts\python.exe")))
else {
    # Virtual environment exists - check if packages are installed
    Write-Host "Virtual environment found. Verifying packages..." -ForegroundColor White
    $VenvPythonExe = Join-Path $VenvPath "Scripts\python.exe"
    $RequirementsPath = Join-Path $ScriptDir "requirements.txt"
    
    # Check if azure-storage-file-datalake is installed
    $TestResult = & $VenvPythonExe -m pip show azure-storage-file-datalake 2>&1
    if ($LASTEXITCODE -ne 0 -or $TestResult -like "*not found*") {
        Write-Host "Required packages not found. Installing..." -ForegroundColor Yellow
        
        if (Test-Path $RequirementsPath) {
            Push-Location $ScriptDir
            try {
                # Upgrade pip first using python -m pip (avoids file lock issues)
                Write-Host "  Upgrading pip..." -ForegroundColor Gray
                $null = & $VenvPythonExe -m pip install --upgrade pip --quiet 2>&1
                
                # Install from requirements.txt
                Write-Host "  Installing dependencies..." -ForegroundColor Gray
                
                # Temporarily disable error action preference to allow pip warnings
                $PrevErrorPref = $ErrorActionPreference
                $ErrorActionPreference = "Continue"
                
                try {
                    $InstallOutput = & $VenvPythonExe -m pip install -r $RequirementsPath 2>&1 | ForEach-Object { $_.ToString() }
                }
                finally {
                    $ErrorActionPreference = $PrevErrorPref
                }
                
                # Convert output to string for analysis
                $OutputText = $InstallOutput -join "`n"
                
                # Check for actual installation failures
                $HasRealError = $OutputText -match "ERROR:.*failed to build" -or 
                               $OutputText -match "Could not install packages" -or
                               $OutputText -match "ERROR:.*No matching distribution"
                
                # Check for successful installation
                $HasSuccess = $OutputText -match "Successfully installed"
                
                if ($HasRealError) {
                    Write-Host "    ERROR: Package installation failed!" -ForegroundColor Red
                    $InstallOutput | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
                    Pop-Location
                    Write-Host "`n================================================================================" -ForegroundColor Cyan
                    exit 1
                }
                elseif ($HasSuccess) {
                    Write-Host "    All packages installed successfully" -ForegroundColor Green
                    # Show dependency warnings if any (but don't fail)
                    if ($OutputText -match "dependency conflicts") {
                        Write-Host "    Note: Some dependency version conflicts detected (non-fatal)" -ForegroundColor Yellow
                    }
                }
                else {
                    # Uncertain - show output
                    $InstallOutput | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
                }
            }
            finally {
                Pop-Location
            }
        }
    }
    else {
        Write-Host "Packages verified successfully" -ForegroundColor Green
    }
}

# Use the virtual environment Python
if (Test-Path (Join-Path $VenvPath "Scripts\python.exe")) {
    $PythonExe = Join-Path $VenvPath "Scripts\python.exe"
}
elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $PythonExe = (Get-Command python).Source
}
elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $PythonExe = "py"
}
else {
    Write-Host "Python not found!" -ForegroundColor Red
    Write-Host "`n================================================================================" -ForegroundColor Cyan
    exit 1
}

Write-Host

# Initialize variables that may be used across steps
$TempFolderPath = $null
$CobFolderNames = @()

# =============================================
# STEP 1: Get list of files to upload
# =============================================
if ($RunStep1) {
    Write-Host
    Write-Host "`n================================================================================`nSTEP 1: Get Upload File List from Snowflake`n================================================================================" -ForegroundColor Cyan

    $Step1Script = Join-Path $ScriptDir "Get_File_List.py"

    if (-not (Test-Path $Step1Script)) {
        Write-Host "Get_File_List.py not found at: $Step1Script" -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit 1
    }

    $Step1Args = @($Step1Script, $Environment, $SnowflakeDB, $CobIdStart.ToString(), $CobIdEnd.ToString())
    
    Write-Host "Running Step 1: Get file list from Snowflake..." -ForegroundColor White
    
    # Run Python directly - NO logger wrapper
    $AllOutput = & $PythonExe $Step1Args 2>&1 | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
        $_
    }
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Get_File_List.py failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit $LASTEXITCODE
    }
    
    # Extract temp folder path and COB folder names from output
    $OutputLines = $AllOutput | Where-Object { $_ -match '^(TEMP_FOLDER|COB_FOLDERS):' }
    
    $TempFolderLine = $OutputLines | Where-Object { $_ -match '^TEMP_FOLDER:' } | Select-Object -Last 1
    $CobFoldersLine = $OutputLines | Where-Object { $_ -match '^COB_FOLDERS:' } | Select-Object -Last 1
    
    if (-not $TempFolderLine -or -not $TempFolderLine.ToString().StartsWith("TEMP_FOLDER:")) {
        Write-Host "Failed to get temp folder path from script output" -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit 1
    }
    
    $TempFolderPath = $TempFolderLine.ToString().Replace("TEMP_FOLDER:", "").Trim()
    
    # Extract COB folder names
    $CobFolderNames = @()
    if ($CobFoldersLine -and $CobFoldersLine.ToString().StartsWith("COB_FOLDERS:")) {
        $FoldersString = $CobFoldersLine.ToString().Replace("COB_FOLDERS:", "").Trim()
        if ($FoldersString) {
            $CobFolderNames = $FoldersString -split ',' | ForEach-Object { $_.Trim() }
        }
    }
    
    Write-Host "Step 1 completed successfully" -ForegroundColor Green
    Write-Host "Temp folder: $TempFolderPath" -ForegroundColor White
    if ($CobFolderNames.Count -gt 0) {
        Write-Host "COB folders created: $($CobFolderNames.Count)" -ForegroundColor White
    }

    Write-Host
}

# =============================================
# STEP 2: Upload files to Azure Data Lake
# =============================================
function Connect-AzWithCertificate {
    # Authenticates Az PowerShell with a Service Principal + cert from LocalMachine\My,
    # in a way that AzCopy (child process, AZCOPY_AUTO_LOGIN_TYPE=PSCRED) can reuse.
    #
    # Why the autosave/clear dance: under a service or scheduled-task account, Az PS
    # cannot persist context to the default user-profile location, so AzCopy can't see
    # the token. Process-scope autosave fixes this; clearing first drops stale context.
    param(
        [Parameter(Mandatory=$true)]  [string] $TenantId,
        [Parameter(Mandatory=$true)]  [string] $ApplicationId,
        [Parameter(Mandatory=$true)]  [string] $CertificateName,
        [Parameter(Mandatory=$false)] [string] $SubscriptionId
    )

    Enable-AzContextAutosave -Scope Process | Out-Null
    Clear-AzContext          -Scope Process -Force | Out-Null

    $cert = Get-ChildItem Cert:\LocalMachine\My |
        Where-Object {
            ($_.Subject -like "*$CertificateName*" -or $_.FriendlyName -eq $CertificateName) -and
            $_.HasPrivateKey -and $_.NotAfter -gt (Get-Date)
        } |
        Sort-Object -Property NotAfter -Descending |
        Select-Object -First 1

    if (-not $cert) {
        throw "No valid certificate matching '$CertificateName' found in Cert:\LocalMachine\My (must have private key and NotAfter in the future)."
    }

    Write-Host "Found certificate: Subject='$($cert.Subject)' Thumbprint=$($cert.Thumbprint) Expires=$($cert.NotAfter)" -ForegroundColor Green

    Connect-AzAccount -ServicePrincipal `
        -Tenant               $TenantId `
        -ApplicationId        $ApplicationId `
        -CertificateThumbprint $cert.Thumbprint | Out-Null

    if ($SubscriptionId) {
        Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null
    }

    # Pre-flight: confirm Connect-AzAccount actually produced a usable context.
    # We deliberately do NOT call Get-AzAccessToken here - the storage token is fetched
    # by AzCopy itself via PSCRED, which is the whole point of this auth flow.
    $ctx = Get-AzContext
    if (-not $ctx -or -not $ctx.Account) {
        throw "Get-AzContext returned no account after Connect-AzAccount."
    }
    Write-Host "Az context OK: Account=$($ctx.Account.Id) Tenant=$($ctx.Tenant.Id) Subscription=$($ctx.Subscription.Id)" -ForegroundColor Green

    return $cert
}

function CheckAzCopyLogForErrors {
    param(
      [Parameter(Mandatory = $true)] [string] $azlogfile
    )

    $errordescr_emptyfolder = Select-String "ERROR: No transfers were scheduled" $azlogfile
    if ($errordescr_emptyfolder) {
        Write-Warning "AzCopy detected an empty folder, it was skipped, but an error was still logged to $azlogfile"
        Write-Warning $errorstr
        Write-Warning $errordescr_emptyfolder
        return "EMPTYFOLDER"
    }

    $jobCompleted = Select-String "Final Job Status: Complete" $azlogfile
    if($jobCompleted){
        return "JOB_COMPLETED"
    }
    return "UNKNOWN"
}

function RunAzCopy {
    param(
      [Parameter(Mandatory = $true)] [string[]] $azcommand,
      [Parameter(Mandatory = $false)] [bool] $ExitOnAzError = $true,
      [Parameter(Mandatory = $false)] [string[]] $ignorelist = @()
    )
    $global:LASTEXITCODE = 0    
    # Use a local variable for script root to avoid assigning to $PSScriptRoot
    $scriptRoot = $PSScriptRoot
    if (-not $scriptRoot) {
        $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    }

    $AzCopyExe = Join-Path $scriptRoot "AzCopy/azcopy.exe"
    Write-Host "AzCopy path: $AzCopyExe"
    if (-not (Test-Path $AzCopyExe)) {
        Write-Error "AzCopy executable not found at $AzCopyExe. Please ensure azcopy.exe is present."
        exit 1
    }

    Write-Host "Running: $AzCopyExe $($azcommand -join ' ')"
    $azcopy = & $AzCopyExe @azcommand
    $logfileMatch = [regex]::match($azcopy, 'Log file is located at: (.+\.log)')
    if ($logfileMatch.Success) {
        Write-Host "AzCopy log: $($logfileMatch.Groups[1].Value)"
    }
    
    # $azerror = CheckAzCopyLogForErrors -azlogfile:$azlogfile
    
    # $limit = (Get-Date).AddDays(-1)
    # $path = $env:AZCOPY_LOG_LOCATION

    # # Delete files older than the $limit.
    # Get-ChildItem -Path $path -Recurse -Force | Where-Object {$_.Name -Like "*.log" -and !$_.PSIsContainer -and $_.CreationTime -lt $limit } | Remove-Item -Force

    # if($azerror -eq "JOB_COMPLETED"){return $azlogfile} else{ return $azerror}

}

if ($RunStep2) {
    Write-Host
    Write-Host "`n================================================================================`nSTEP 2: Upload Files to Azure Data Lake`n================================================================================" -ForegroundColor Cyan

    if ($Environment -ne "prod") {
        # Non-prod path: AzCopy with Service Principal cert auth (Az PS PSCRED bridge).
        Write-Host "Authentication Method: Certificate (Service Principal via Az PowerShell + AzCopy PSCRED)" -ForegroundColor White

        if (-not $TempFolderPath) {
            Write-Host "Temp folder path not available. Step 1 must be run first." -ForegroundColor Red
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit 1
        }

        try {
            Connect-AzWithCertificate `
                -TenantId        $TenantId `
                -ApplicationId   $ServicePrincipalId `
                -CertificateName $CertName `
                -SubscriptionId  $SubscriptionId | Out-Null
        }
        catch {
            Write-Error "Azure authentication failed: $($_.Exception.Message)"
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit 1
        }

        # Tell AzCopy to reuse the Az PowerShell session token (PSCRED).
        $env:AZCOPY_AUTO_LOGIN_TYPE = 'PSCRED'
        $env:AZCOPY_LOG_LOCATION    = Join-Path $PSScriptRoot "AzCopy\Log"

        $OverwriteAzCopy = switch ($OverwriteOption.ToUpper()) {
            'TRUE'  { '--overwrite=True' }
            'FALSE' { '--overwrite=False' }
            default { '--overwrite=IfSourceNewer' }   # SYNC (default)
        }

        if (-not $CobFolderNames -or $CobFolderNames.Count -eq 0) {
            Write-Host "No COB folders to upload (Step 1 produced none)." -ForegroundColor Yellow
        }
        else {
            foreach ($cobFolderName in $CobFolderNames) {
                $cobFolderPath = Join-Path $TempFolderPath $cobFolderName
                if (-not (Test-Path $cobFolderPath)) {
                    Write-Host "Skipping missing COB folder: $cobFolderPath" -ForegroundColor Yellow
                    continue
                }

                Write-Host "`n--- Uploading COB folder: $cobFolderName ---" -ForegroundColor Cyan

                Get-ChildItem -Path $cobFolderPath -Filter *.csv | ForEach-Object {
                    $csvFile  = $_
                    $fileBase = [System.IO.Path]::GetFileNameWithoutExtension($csvFile.Name)
                    $InputFileList = Join-Path $cobFolderPath ("{0}_{1}.txt" -f $fileBase, (Get-Random))

                    $csv = Import-Csv $csvFile.FullName
                    if (-not $csv) {
                        Write-Host "  Empty CSV: $($csvFile.Name) - skipping" -ForegroundColor Yellow
                        return
                    }

                    $sourceFilePath      = ($csv[0].'SOURCE_FILE_PATH').TrimEnd('/').TrimEnd('\')
                    $destinationFilePath = ($csv[0].'DESTINATION_FILE_PATH').Trim('/')

                    $csv | ForEach-Object { $_.'FILE_NAME' } |
                        Out-File -Encoding UTF8 -FilePath $InputFileList

                    if (-not (Test-Path $InputFileList) -or -not (Get-Content $InputFileList)) {
                        Write-Host "  No file names in $($csvFile.Name) - skipping" -ForegroundColor Yellow
                        if (Test-Path $InputFileList) { Remove-Item $InputFileList -Force }
                        return
                    }

                    # ADLS Gen2 endpoint is dfs.core.windows.net (not blob).
                    $TargetPath = "https://$StorageAccountName.dfs.core.windows.net/$ContainerName/$destinationFilePath"

                    Write-Host ""
                    Write-Host "-------------- Execute Sync --------------"
                    Write-Host "CSV              : $($csvFile.Name)"
                    Write-Host "Input list       : $InputFileList"
                    Write-Host "Source path      : $sourceFilePath"
                    Write-Host "Destination path : $destinationFilePath"
                    Write-Host "Data Lake Path   : $TargetPath"
                    Write-Host ""

                    try {
                        $entries = Get-Content $InputFileList
                        if ($entries -imatch '\*') {
                            Write-Host "Wildcard pattern detected - copying per entry"
                            foreach ($entry in $entries) {
                                Write-Host "  $entry"
                                $azcopy_args = @(
                                    'copy',
                                    "$sourceFilePath\$entry",
                                    $TargetPath,
                                    '--recursive',
                                    $OverwriteAzCopy,
                                    '--output-type=text'
                                )
                                RunAzCopy -azcommand $azcopy_args -ExitOnAzError $false
                            }
                        }
                        else {
                            Write-Host "No wildcards - using --list-of-files"
                            $azcopy_args = @(
                                'copy',
                                "$sourceFilePath\*",
                                $TargetPath,
                                '--recursive',
                                $OverwriteAzCopy,
                                '--output-type=text',
                                '--list-of-files', $InputFileList
                            )
                            RunAzCopy -azcommand $azcopy_args -ExitOnAzError $false
                        }
                    }
                    catch {
                        Write-Error "Error during AzCopy for $($csvFile.Name): $($_.Exception.Message)"
                    }
                    finally {
                        if (Test-Path $InputFileList) { Remove-Item $InputFileList -Force }
                    }
                }
            }
        }

        Write-Host "Step 2 (AzCopy) completed" -ForegroundColor Green
    }
    else {
        # Non-prod: Use Python script with Service Principal authentication
        $Step2Script = Join-Path $ScriptDir "Azure_DataLake_Operations.py"

        if (-not (Test-Path $Step2Script)) {
            Write-Host "Azure_DataLake_Operations.py not found at: $Step2Script" -ForegroundColor Red
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit 1
        }

        if (-not $TempFolderPath) {
            Write-Host "Temp folder path not available. Step 1 must be run first." -ForegroundColor Red
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit 1
        }

        # Display authentication method
        Write-Host "Authentication Method: $AuthMethod" -ForegroundColor White
        if ($AuthMethod -eq "Certificate") {
            # DEBUG: Certificate: $CertName
            # DEBUG: Service Principal: $ServicePrincipalId
        }

        # Call Python script to upload files
        $Step2Args = @(
            $Step2Script,
            "upload",
            $Environment,
            $SnowflakeDB,
            $StageFilter,
            $CobIdStart.ToString(),
            $CobIdEnd.ToString(),
            $StorageAccountName,
            $TenantId,
            $ServicePrincipalId,
            $CertName,
            $AuthMethod,
            $TempFolderPath
        )

        Write-Host "Running Step 2: Upload files to Azure Data Lake..." -ForegroundColor White
        
        # Use try/finally to ensure cleanup runs even if upload fails
        $Step2ExitCode = 0
        try {
            # Run Python directly - NO logger wrapper
            & $PythonExe $Step2Args 2>&1 | ForEach-Object {
                Write-Host "  $_" -ForegroundColor Gray
            }
            $Step2ExitCode = $LASTEXITCODE
        }
        finally {
            # Always clean up temporary COB folders, even if upload failed
            if ($TempFolderPath -and (Test-Path $TempFolderPath) -and $CobFolderNames -and $CobFolderNames.Count -gt 0) {
                Write-Host "Cleaning up temporary COB folders..." -ForegroundColor White
                
                $DeletedCount = 0
                $FailedCount = 0
                
                foreach ($FolderName in $CobFolderNames) {
                    $FolderPath = Join-Path $TempFolderPath $FolderName
                    
                    if (Test-Path $FolderPath) {
                        try {
                            # DEBUG: Deleting: $FolderName
                            Remove-Item -Path $FolderPath -Recurse -Force -ErrorAction Stop
                            $DeletedCount++
                        }
                        catch {
                            Write-Host "Failed to delete $FolderName : $($_.Exception.Message)" -ForegroundColor Yellow
                            $FailedCount++
                        }
                    }
                }
                
                if ($DeletedCount -gt 0) {
                    Write-Host "Cleanup completed: $DeletedCount folder(s) deleted" -ForegroundColor Green
                }
                if ($FailedCount -gt 0) {
                    Write-Host "$FailedCount folder(s) failed to delete" -ForegroundColor Yellow
                }
            }
            elseif (-not $CobFolderNames -or $CobFolderNames.Count -eq 0) {
                # DEBUG: No COB folders to clean up (Step 1 was not run or no folders created)
            }
        }
        
        # Exit if Step 2 failed
        if ($Step2ExitCode -ne 0) {
            Write-Host "Step 2 failed - exiting after cleanup" -ForegroundColor Red
            Write-Host "`n================================================================================" -ForegroundColor Cyan
            exit $Step2ExitCode
        }

        Write-Host "Step 2 completed successfully" -ForegroundColor Green
    }

    Write-Host
}
else {
    # DEBUG: STEP 2: Skipped
}

# =============================================
# STEP 3: List files and save to Snowflake
# =============================================
if ($RunStep3) {
    Write-Host
    Write-Host "`n================================================================================`nSTEP 3: List Azure Files and Save to Snowflake`n================================================================================" -ForegroundColor Cyan

    $Step3Script = Join-Path $ScriptDir "Azure_DataLake_Operations.py"

    if (-not (Test-Path $Step3Script)) {
        Write-Host "Azure_DataLake_Operations.py not found at: $Step3Script" -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit 1
    }

    # Display authentication method
    Write-Host "Authentication Method: $AuthMethod" -ForegroundColor White
    if ($AuthMethod -eq "Certificate") {
        # DEBUG: Certificate: $CertName
        # DEBUG: Service Principal: $ServicePrincipalId
    }

    # Call Python script to list and save
    $Step3Args = @(
        $Step3Script,
        "list",
        $Environment,
        $SnowflakeDB,
        $StageFilter,
        $CobIdStart.ToString(),
        $CobIdEnd.ToString(),
        $StorageAccountName,
        $TenantId,
        $ServicePrincipalId,
        $CertName,
        $AuthMethod
    )

    Write-Host "Running Step 3: List files and save to Snowflake..." -ForegroundColor White
    
    # Run Python directly - NO logger wrapper
    & $PythonExe $Step3Args 2>&1 | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
    }
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Azure_DataLake_Operations.py (list) failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "`n================================================================================" -ForegroundColor Cyan
        exit $LASTEXITCODE
    }

    Write-Host "Step 3 completed successfully" -ForegroundColor Green
    Write-Host
}
else {
    # DEBUG: STEP 3: Skipped
}

# =============================================
# FINAL SUMMARY
# =============================================
Write-Host
Write-Host "`n================================================================================`nWORKFLOW COMPLETED SUCCESSFULLY!`n================================================================================" -ForegroundColor Cyan
Write-Host

Write-Host "`n--- Summary ---" -ForegroundColor Cyan
if ($RunStep1) {
    Write-Host "1. File list saved to: $TempFolderPath" -ForegroundColor White
} else {
    # DEBUG: 1. File list: Skipped
}
if ($RunStep2) {
    if ($Environment -eq "prod") {
        Write-Host "2. Upload: Placeholder (to be implemented)" -ForegroundColor White
    }
    else {
        Write-Host "2. Upload completed and temp folders cleaned up" -ForegroundColor White
    }
} else {
    # DEBUG: 2. Upload: Skipped
}
if ($RunStep3) {
    Write-Host "3. Files listed and saved to Snowflake" -ForegroundColor White
} else {
    # DEBUG: 3. List and save: Skipped
}

Write-Host "`n================================================================================" -ForegroundColor Cyan
exit 0



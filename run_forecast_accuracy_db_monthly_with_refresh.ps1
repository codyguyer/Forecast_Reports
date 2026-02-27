param(
    [string]$Month = $null,
    [string]$Server = "(localdb)\MSSQLLocalDB",
    [string]$Database = "Forecast_Database",
    [string]$Driver = "ODBC Driver 17 for SQL Server",
    [string]$MarketingFile = (Join-Path $PSScriptRoot "Marketing Forecast Data.xlsx"),
    [string]$MarketingSnapshotMonth = (Get-Date -Format "yyyy-MM"),
    [string]$EssbaseUsername = "cguyer",
    [string]$EssbasePassword = $env:ESSBASE_PASSWORD,
    [string]$CompareBaseline = "legacy",
    [string]$DQMode = "fail",
    [int]$RefreshAttempts = 3,
    [switch]$SkipActualsLoad,
    [switch]$SkipCatalogLoad,
    [switch]$VisibleExcel
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$refreshScript = Join-Path $PSScriptRoot "refresh_marketing_forecast_essbase.py"
$runnerScript = Join-Path $PSScriptRoot "run_forecast_accuracy_db_monthly.py"

if (-not (Test-Path $refreshScript)) {
    throw "Missing script: $refreshScript"
}
if (-not (Test-Path $runnerScript)) {
    throw "Missing script: $runnerScript"
}
if (-not (Test-Path $MarketingFile)) {
    throw "Marketing workbook not found: $MarketingFile"
}
if ([string]::IsNullOrWhiteSpace($EssbasePassword)) {
    Write-Host "Essbase password not found in -EssbasePassword or ESSBASE_PASSWORD."
    $securePwd = Read-Host "Enter Essbase password for this run (input hidden; not persisted)" -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePwd)
    try {
        $EssbasePassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
    if ([string]::IsNullOrWhiteSpace($EssbasePassword)) {
        throw "Essbase password is required."
    }
}
$env:ESSBASE_PASSWORD = $EssbasePassword

$beforeWrite = (Get-Item $MarketingFile).LastWriteTimeUtc
Write-Host "Step 1/4: Refresh marketing workbook from Smart View/Eessbase..."

$refreshArgs = @(
    $refreshScript,
    "--workbook", $MarketingFile,
    "--username", $EssbaseUsername,
    "--password", $EssbasePassword,
    "--sheet-name", "2026 Data"
)
if ($VisibleExcel) {
    $refreshArgs += "--visible"
}

if ($RefreshAttempts -lt 1) {
    throw "RefreshAttempts must be >= 1."
}

$refreshSucceeded = $false
for ($attempt = 1; $attempt -le $RefreshAttempts; $attempt++) {
    Write-Host ("Essbase refresh attempt {0}/{1}..." -f $attempt, $RefreshAttempts)
    python @refreshArgs
    if ($LASTEXITCODE -eq 0) {
        $refreshSucceeded = $true
        break
    }
    if ($attempt -lt $RefreshAttempts) {
        Write-Host "Essbase refresh attempt failed. Waiting 5 seconds before retry..."
        Start-Sleep -Seconds 5
    }
}

if (-not $refreshSucceeded) {
    throw "Essbase refresh failed after $RefreshAttempts attempt(s)."
}

$afterWrite = (Get-Item $MarketingFile).LastWriteTimeUtc
if ($afterWrite -le $beforeWrite) {
    throw "Workbook timestamp did not change after refresh. Before: $beforeWrite | After: $afterWrite"
}
Write-Host "Workbook refresh timestamp check passed. Updated at UTC: $afterWrite"

Write-Host "Step 2/4: Dry-run parse validation for marketing loader..."
$dryArgs = @(
    "c:\Users\cguyer\OneDrive - Midmark Corporation\Documents\Sales Ops\Demand Planning Projects\Python Modeling\demand_forecast_prototype_v4\scripts\sql\load_marketing_forecast_2026_to_sql.py",
    "--server", $Server,
    "--database", $Database,
    "--driver", $Driver,
    "--source-file", $MarketingFile,
    "--sheet", "2026 Data",
    "--start-row", "6",
    "--snapshot-month", $MarketingSnapshotMonth,
    "--dry-run"
)
python @dryArgs
if ($LASTEXITCODE -ne 0) {
    throw "Marketing loader dry-run failed with exit code $LASTEXITCODE"
}

Write-Host "Step 3/4: Execute DB monthly runbook..."
$runArgs = @(
    $runnerScript,
    "--server", $Server,
    "--database", $Database,
    "--driver", $Driver,
    "--marketing-file", $MarketingFile,
    "--marketing-snapshot-month", $MarketingSnapshotMonth,
    "--compare-baseline", $CompareBaseline,
    "--dq-mode", $DQMode
)

if (-not [string]::IsNullOrWhiteSpace($Month)) {
    $runArgs += @("--month", $Month)
}
if ($SkipActualsLoad) {
    $runArgs += "--skip-actuals-load"
}
if ($SkipCatalogLoad) {
    $runArgs += "--skip-catalog-load"
}

python @runArgs
if ($LASTEXITCODE -ne 0) {
    throw "DB monthly run failed with exit code $LASTEXITCODE"
}

Write-Host "Step 4/4: Completed successfully."

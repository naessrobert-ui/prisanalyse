param(
    [string]$Fra = "2026-04-01",
    [string]$Til = (Get-Date).ToString("yyyy-MM-dd"),
    [switch]$Torrkjor,
    [int]$PauseSekunder = 10
)

# UTF-8 for både PowerShell-output, Tee-Object og Python når stdout/stderr pipes.
# Dette hindrer typiske Windows-feil rundt tegn som ø, æ og pil-tegn.
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$ErrorActionPreference = "Stop"

# Sørg for at scriptet kjører fra samme mappe som denne .ps1-filen.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$PythonScript = Join-Path $ScriptDir "sync_regnskap.py"
$LogDir = Join-Path $ScriptDir "logs"

if (!(Test-Path $PythonScript)) {
    throw "Fant ikke sync_regnskap.py i $ScriptDir"
}

if (!(Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$StartDato = [datetime]::ParseExact($Fra, "yyyy-MM-dd", $null)
$SluttDato = [datetime]::ParseExact($Til, "yyyy-MM-dd", $null)

if ($StartDato -gt $SluttDato) {
    throw "-Fra kan ikke være etter -Til"
}

Write-Host "============================================================"
Write-Host "Backfill regnskap dag-for-dag"
Write-Host "Periode:  $($StartDato.ToString('yyyy-MM-dd')) -> $($SluttDato.ToString('yyyy-MM-dd'))"
Write-Host "Tørrkjør: $Torrkjor"
Write-Host "Loggmappe: $LogDir"
Write-Host "============================================================"

$DagensDato = $StartDato

while ($DagensDato -le $SluttDato) {
    $DatoStr = $DagensDato.ToString("yyyy-MM-dd")
    $LogFil = Join-Path $LogDir "sync_regnskap_$DatoStr.log"

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "Starter $DatoStr"
    Write-Host "Logg: $LogFil"
    Write-Host "============================================================"

    $Args = @(
        $PythonScript,
        "--fra", $DatoStr,
        "--til", $DatoStr,
        "--steg", "1"
    )

    if ($Torrkjor) {
        $Args += "--tørrkjør"
    }

    $StartTid = Get-Date

    # Viktig:
    # I Windows PowerShell kan stderr fra native programmer bli behandlet som ErrorRecord.
    # Derfor skrur vi midlertidig av Stop, konverterer alt til tekst og logger hele tracebacken.
    $OldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        & python @Args 2>&1 |
            ForEach-Object { $_.ToString() } |
            Tee-Object -FilePath $LogFil

        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $OldEap
    }

    $SluttTid = Get-Date
    $Varighet = New-TimeSpan -Start $StartTid -End $SluttTid

    if ($ExitCode -ne 0) {
        Write-Host ""
        Write-Host "FEIL: Jobben for $DatoStr stoppet med exit code $ExitCode" -ForegroundColor Red
        Write-Host "Se logg: $LogFil"
        exit $ExitCode
    }

    Write-Host ""
    Write-Host "Ferdig $DatoStr på $([math]::Round($Varighet.TotalMinutes, 1)) minutter"

    if ($PauseSekunder -gt 0 -and $DagensDato -lt $SluttDato) {
        Write-Host "Pause $PauseSekunder sekunder før neste dag..."
        Start-Sleep -Seconds $PauseSekunder
    }

    $DagensDato = $DagensDato.AddDays(1)
}

Write-Host ""
Write-Host "============================================================"
Write-Host "Alle dagsjobber er ferdige."
Write-Host "============================================================"

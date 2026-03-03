param(
    [string]$PythonExe = "",
    [string]$InputModels = "input/signed_dags_models3.txt",
    [string]$OutputBase = "signed_dags_models3",
    [string[]]$TracePredictions = @("D,L|E", "D,L|V", "L,E|V")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-Python([string]$Preferred) {
    $candidates = @()
    if ($Preferred) { $candidates += $Preferred }
    if ($env:PYTHON_EXE) { $candidates += $env:PYTHON_EXE }
    $candidates += "$env:USERPROFILE\AppData\Local\anaconda3\python.exe"
    $candidates += "python"

    foreach ($cand in $candidates) {
        try {
            & $cand --version *> $null
            if ($LASTEXITCODE -eq 0) {
                return $cand
            }
        } catch {
            # try next candidate
        }
    }
    throw "No working Python interpreter found. Pass -PythonExe `"C:\path\to\python.exe`"."
}

$root = $PSScriptRoot
$progDir = Join-Path $root "prog"
$inputPath = Join-Path $root $InputModels
$outputDir = Join-Path $root "output"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

if (!(Test-Path $inputPath)) {
    throw "Input model file not found: $inputPath"
}

$python = Resolve-Python $PythonExe
Write-Host "Using Python: $python"

$modelsJson = Join-Path $outputDir "$OutputBase`_output.json"
$compareCsv = Join-Path $outputDir "$OutputBase`_comparison.csv"
$comparePdf = Join-Path $outputDir "$OutputBase`_comparison.pdf"
$traceJson = Join-Path $outputDir "$OutputBase`_trace.json"
$traceSummaryCsv = Join-Path $outputDir "$OutputBase`_trace_summary.csv"
$traceSummaryPdf = Join-Path $outputDir "$OutputBase`_trace_summary.pdf"
$tracePathsCsv = Join-Path $outputDir "$OutputBase`_trace_paths.csv"
$tracePathsPdf = Join-Path $outputDir "$OutputBase`_trace_paths.pdf"

& $python (Join-Path $progDir "dag_implications.py") `
    --signed-models-file $inputPath `
    --output-json $modelsJson

& $python (Join-Path $progDir "json_to_table.py") `
    $modelsJson `
    --output-csv $compareCsv `
    --output-pdf $comparePdf

$traceArgs = @(
    (Join-Path $progDir "dag_implications.py"),
    "--signed-models-file", $inputPath
)
foreach ($tp in $TracePredictions) {
    $traceArgs += @("--trace-prediction", $tp)
}
$traceArgs += @("--output-json", $traceJson)
& $python @traceArgs

& $python (Join-Path $progDir "trace_json_to_table.py") `
    $traceJson `
    --summary-csv $traceSummaryCsv `
    --summary-pdf $traceSummaryPdf `
    --paths-csv $tracePathsCsv `
    --paths-pdf $tracePathsPdf

Write-Host ""
Write-Host "Done. Outputs:"
Write-Host "  $modelsJson"
Write-Host "  $compareCsv"
Write-Host "  $comparePdf"
Write-Host "  $traceJson"
Write-Host "  $traceSummaryCsv"
Write-Host "  $traceSummaryPdf"
Write-Host "  $tracePathsCsv"
Write-Host "  $tracePathsPdf"

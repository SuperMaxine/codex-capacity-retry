param(
    [double]$RetryDelay = 60,
    [double]$PollInterval = 5,
    [int]$MaxPerMinute = 6,
    [string[]]$ThreadId = @(),
    [switch]$Execute,
    [switch]$Once,
    [double]$RunFor = 0
)
$ErrorActionPreference = 'Stop'
$retryPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $retryPython)) {
    $retryPython = (Get-Command python -ErrorAction Stop).Source
}
$retryArgs = @(
    (Join-Path $PSScriptRoot 'codex_native_retry.py'),
    '--retry-delay', $RetryDelay.ToString([System.Globalization.CultureInfo]::InvariantCulture),
    '--poll-interval', $PollInterval.ToString([System.Globalization.CultureInfo]::InvariantCulture),
    '--max-per-minute', $MaxPerMinute
)
if ($Execute) { $retryArgs += '--execute' }
if ($Once) { $retryArgs += '--once' }
if ($RunFor -gt 0) { $retryArgs += @('--run-for', $RunFor.ToString([System.Globalization.CultureInfo]::InvariantCulture)) }
foreach ($retryThreadId in $ThreadId) { $retryArgs += @('--thread', $retryThreadId) }
& $retryPython @retryArgs
exit $LASTEXITCODE

function Get-PythonCommand {
  if ($env:ATLAS_PYTHON) {
    return @($env:ATLAS_PYTHON)
  }

  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) {
    return @($py.Source, "-3")
  }

  $python = Get-Command python -ErrorAction SilentlyContinue
  if ($python) {
    return @($python.Source)
  }

  throw "Python was not found. Set ATLAS_PYTHON or install python/py on PATH."
}

$python = Get-PythonCommand

function Invoke-Python {
  param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Arguments
  )

  $prefix = @()
  if ($python.Count -gt 1) {
    $prefix = $python[1..($python.Count - 1)]
  }

  & $python[0] @prefix @Arguments
}

$required = @("flask")
$missing = @()

foreach ($module in $required) {
  Invoke-Python -c "import $module" 2>$null
  if ($LASTEXITCODE -ne 0) {
    $missing += $module
  }
}

if ($missing.Count -gt 0) {
  Write-Host "Installing missing Python packages..."
  Invoke-Python -m pip install -r requirements.txt
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}

Invoke-Python app.py

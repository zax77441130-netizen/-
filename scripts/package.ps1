param(
  [string]$Source = (Resolve-Path "$PSScriptRoot\..").Path,
  [string]$Destination = ""
)

$ErrorActionPreference = "Stop"

if (-not $Destination) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $Destination = Join-Path (Split-Path -Parent $Source) "recruitops-$stamp-full-clean.zip"
}

$sourcePath = (Resolve-Path -LiteralPath $Source).Path
$staging = Join-Path $env:TEMP ("recruitops-package-" + [guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $staging | Out-Null

$excludedDirs = @(
  "\__pycache__\",
  "\.pytest_cache\",
  "\postgres_data\",
  "\redis_data\",
  "\node_modules\",
  "\dist\",
  "\data\inbound_mail\",
  "\data\resumes\",
  "\data\screenshots\",
  "\data\sessions\",
  "\local_private\"
)
$excludedNamePatterns = @(
  "^recruitment\.db$",
  ".*\.db$",
  ".*\.sqlite$",
  ".*\.sqlite3$",
  ".*\.pyc$",
  ".*\.pyo$",
  ".*\.log$",
  ".*\.zip$",
  ".*session.*\.(json|zip|txt)$",
  ".*cookie.*\.(json|txt)$",
  ".*token.*\.(json|txt|env)$",
  ".*resume.*\.(pdf|docx|doc)$",
  "^\.env$"
)

try {
  Get-ChildItem -LiteralPath $sourcePath -Recurse -File | ForEach-Object {
    $rel = $_.FullName.Substring($sourcePath.Length + 1)
    $relForMatch = "\" + $rel
    foreach ($dir in $excludedDirs) {
      if ($relForMatch.Contains($dir)) { return }
    }
    foreach ($pattern in $excludedNamePatterns) {
      if ($_.Name -match $pattern) { return }
    }
    $target = Join-Path $staging $rel
    New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
    Copy-Item -LiteralPath $_.FullName -Destination $target
  }

  if (Test-Path -LiteralPath $Destination) {
    Remove-Item -LiteralPath $Destination -Force
  }
  Add-Type -AssemblyName System.IO.Compression.FileSystem
  Add-Type -AssemblyName System.IO.Compression
  $zip = [IO.Compression.ZipFile]::Open($Destination, [IO.Compression.ZipArchiveMode]::Create)
  try {
    Get-ChildItem -LiteralPath $staging -Recurse -File | ForEach-Object {
      $entryName = $_.FullName.Substring($staging.Length + 1).Replace("\", "/")
      $entry = $zip.CreateEntry($entryName, [IO.Compression.CompressionLevel]::Optimal)
      $sourceStream = [IO.File]::OpenRead($_.FullName)
      try {
        $entryStream = $entry.Open()
        try {
          $sourceStream.CopyTo($entryStream)
        }
        finally {
          $entryStream.Dispose()
        }
      }
      finally {
        $sourceStream.Dispose()
      }
    }
  }
  finally {
    $zip.Dispose()
  }

  $archive = [IO.Compression.ZipFile]::OpenRead($Destination)
  try {
    $badEntries = $archive.Entries |
      Where-Object {
        $_.FullName.Contains("\") -or
        $_.FullName -match '(^|/)(__pycache__|\.pytest_cache|postgres_data|redis_data|node_modules|data/(inbound_mail|resumes|screenshots|sessions)|local_private)/' -or
        $_.Name -match '(^recruitment\.db$|.*\.(db|sqlite|sqlite3|pyc|pyo|log|zip)$|.*session.*\.(json|zip|txt)$|.*cookie.*\.(json|txt)$|.*token.*\.(json|txt|env)$|.*resume.*\.(pdf|docx|doc)$|^\.env$)'
      } |
      Select-Object -ExpandProperty FullName
  }
  finally {
    $archive.Dispose()
  }
  if ($badEntries) {
    Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue
    throw "Package contains forbidden delivery artifacts or non-portable paths: $($badEntries -join ', ')"
  }
  Get-Item -LiteralPath $Destination
}
finally {
  Remove-Item -LiteralPath $staging -Recurse -Force -ErrorAction SilentlyContinue
}

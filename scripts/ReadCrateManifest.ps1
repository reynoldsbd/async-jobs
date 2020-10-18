<#
.SYNOPSIS
Loads crate info from Cargo.toml and stores in GitHub Actions step outputs
#>

[CmdletBinding()]

param ()

foreach ($line in (Get-Content $PSScriptRoot/../Cargo.toml)) {
    if ($line -match "^version = `"(?<version>.*)`"$") {
        Write-Host "::set-output name=version::$($matches.version)"
        return
    }
}

throw "failed to load crate version"

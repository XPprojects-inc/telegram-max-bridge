$ErrorActionPreference = 'Stop'
$bridgePython = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $bridgePython)) {
    py -3 -m venv (Join-Path $PSScriptRoot '.venv')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot create Python environment' }
}
& $bridgePython -m ensurepip
& $bridgePython -m pip install -r (Join-Path $PSScriptRoot 'requirements.txt') pyinstaller==6.22.3
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
& $bridgePython -m PyInstaller --noconfirm --onefile --windowed --name TelegramMaxBridge --collect-all pymax --copy-metadata maxapi-python --add-data "$(Join-Path $PSScriptRoot 'THIRD_PARTY_NOTICES.txt');." --distpath (Join-Path $PSScriptRoot 'dist') --workpath (Join-Path $PSScriptRoot '.build') --specpath (Join-Path $PSScriptRoot '.build') (Join-Path $PSScriptRoot 'desktop.py')
if ($LASTEXITCODE -ne 0) { throw 'EXE build failed' }

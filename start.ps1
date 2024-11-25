# Define the path to the virtual environment activation script
$activateScript = "$PSScriptRoot\.venv\Scripts\Activate.ps1"

# Check if the virtual environment activation script exists
if (-not (Test-Path $activateScript)) {
    Write-Host "Error: Virtual environment activation script not found. Please ensure the virtual environment is set up in .venv."
    exit 1
}

# Activate the virtual environment
Write-Host "Activating virtual environment..."
& $activateScript

# Check if the Python script exists
if (-not (Test-Path ".\program.py")) {
    Write-Host "Error: program.py is missing. Please check your local project files."
    exit 1
}

# Run the Python script
Write-Host "Starting Python program..."
python ".\data_loading\program.py"

# Exit the PowerShell script with the Python script's exit code
exit $LASTEXITCODE
function Test-Python {
    Write-Host "Testing Python... " -NoNewline
    $pythonInstalled = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonInstalled) {
        Write-Host "Python is not installed. Please install Python and try again."
        exit 1
    }
    Write-Host "Python is installed."
}

function Test-Docker {
    Write-Host "Testing Docker... " -NoNewline
    $dockerInstalled = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $dockerInstalled) {
        Write-Host "Docker is not installed. Please install Docker and try again."
        exit 1
    }
    Write-Host "Docker is installed."

    Write-Host "Testing if Docker is running... " -NoNewline
    try {
        docker info > $null 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Docker is running."
        } else {
            Write-Host "Docker is not running. Please start Docker and try again."
            exit 1
        }
    } catch {
        Write-Host "Docker is not running. Please start Docker and try again."
        exit 1
    }
}

function Test-EnvFile {
    Write-Host "Testing for .env file... " -NoNewline
    if (-not (Test-Path ".\.env")) {
        Write-Host ".env file is missing. Please create a .env file and try again."
        exit 1
    }
    Write-Host ".env file is present."
}

function New-VirtualEnv {
    $envPath = ".\.venv"
    if (-not (Test-Path $envPath)) {
        Write-Host "Creating virtual environment... " -NoNewline
        python -m venv $envPath
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Failed to create virtual environment."
            exit 1
        }
        Write-Host "Virtual environment created."
    } else {
        Write-Host "Virtual environment already exists."
    }
}

function Get-VirtualEnv {
    Write-Host "Activating virtual environment... " -NoNewline
    $activateScript = "$PSScriptRoot\.venv\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
        Write-Host "Virtual environment activated."
    } else {
        Write-Host "Activation script not found. Ensure the virtual environment is correctly set up."
        exit 1
    }
}

function Install-PythonPackages {
    Write-Host "Installing Python packages..."
    if (-not (Test-Path ".\requirements.txt")) {
        Write-Host "requirements.txt not found."
        exit 1
    }
    python -m pip install --upgrade pip
    python -m pip install -r ".\requirements.txt"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Python packages installed successfully."
    } else {
        Write-Host "Failed to install Python packages."
        exit 1
    }
}

function Start-DockerContainers {
    Write-Host "Starting Docker containers..."
    docker compose up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Docker containers started successfully."
    } else {
        Write-Host "Failed to start Docker containers."
        exit 1
    }
}

# Main Script Execution
Write-Host "Starting setup..."

Test-Python
Test-Docker
Test-EnvFile
# New-VirtualEnv
# Get-VirtualEnv
# Install-PythonPackages
Start-DockerContainers

Write-Host "Setup completed successfully."

pause
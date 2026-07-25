#!/bin/bash
set -e

echo "=== Starting P.R.I.C.E Deployment ==="

# 1. Update system packages and install Python Virtual Environment dependencies
echo "Updating packages..."
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv git build-essential pkg-config libssl-dev

# 2. Install Rust Toolchain if not already installed
if ! command -v cargo &> /dev/null; then
    echo "Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi

# Load Rust environment
source "$HOME/.cargo/env"
echo "Rust version: $(rustc --version)"

# 3. Pull latest code (in case script is run locally on the server)
echo "Pulling latest code..."
git pull origin main || echo "Git pull skipped or failed, proceeding with current directory files..."

# 4. Build Rust services in release mode
echo "Building Rust workspace in release mode..."
cargo build --release

# 5. Set up Python virtual environment and dependencies
echo "Setting up Python virtual environment..."
cd python-broker
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
cd ..

# 6. Start/Restart processes with PM2
echo "Configuring PM2 services..."

# Delete existing processes if they exist to prevent duplicates (only touching our own processes)
pm2 delete price-python-broker 2>/dev/null || true
pm2 delete price-worker 2>/dev/null || true
pm2 delete price-server 2>/dev/null || true

# Start Python Broker
pm2 start "python-broker/venv/bin/python -m uvicorn python-broker.app:app --host 127.0.0.1 --port 8001" --name price-python-broker

# Start Rust Worker and Server
pm2 start "target/release/price-worker" --name price-worker
pm2 start "target/release/price-server" --name price-server

# Save PM2 state
pm2 save

echo "=== Deployment Completed Successfully ==="
pm2 list

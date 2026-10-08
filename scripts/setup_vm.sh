#!/usr/bin/env bash
# ==============================================================================
# ZERO-TOUCH VM INITIALIZATION SCRIPT FOR GCP ALWAYS-FREE TIER (e2-micro)
# Target OS: Ubuntu 22.04 LTS / 24.04 LTS
# Memory Tuning: Configures 2GB Swap space to safeguard 1GB RAM VM
# Service Management: Installs systemd daemon for 24/7 autonomous streaming
# ==============================================================================

set -euo pipefail

APP_DIR="/opt/flight-weather-engine"
SERVICE_USER="flightengine"
SWAP_FILE="/swapfile"
SYSTEMD_SERVICE="/etc/systemd/system/flight-engine.service"

echo "=== [1/7] Updating Base System Packages ==="
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y python3-pip python3-venv git curl jq ufw libgomp1

echo "=== [2/7] Configuring 2GB Linux Swap Space (OOM Guard for e2-micro) ==="
if [ ! -f "$SWAP_FILE" ]; then
    sudo fallocate -l 2G "$SWAP_FILE"
    sudo chmod 600 "$SWAP_FILE"
    sudo mkswap "$SWAP_FILE"
    sudo swapon "$SWAP_FILE"
    echo "$SWAP_FILE none swap sw 0 0" | sudo tee -a /etc/fstab
    # Lower swappiness to prefer RAM over disk swap when possible
    sudo sysctl vm.swappiness=10
    echo "vm.swappiness=10" | sudo tee -a /etc/sysctl.conf
    echo "Swapfile provisioned and activated successfully."
else
    echo "Swapfile already present; skipping."
fi

echo "=== [3/7] Provisioning Dedicated Service Account and Directory ==="
if ! id "$SERVICE_USER" &>/dev/null; then
    sudo useradd -r -s /bin/bash -d "$APP_DIR" "$SERVICE_USER"
fi
sudo mkdir -p "$APP_DIR"
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_DIR"

echo "=== [4/7] Setting up Python Virtual Environment & Dependencies ==="
if [ ! -d "$APP_DIR/venv" ]; then
    sudo -u "$SERVICE_USER" python3 -m venv "$APP_DIR/venv"
fi

if [ -f "$APP_DIR/requirements.txt" ]; then
    sudo -u "$SERVICE_USER" "$APP_DIR/venv/bin/pip" install --upgrade pip
    sudo -u "$SERVICE_USER" "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt"
fi

echo "=== [5/7] Installing Systemd Autonomous Micro-Batch Daemon ==="
sudo tee "$SYSTEMD_SERVICE" > /dev/null <<EOF
[Unit]
Description=Eco-Friendly Hybrid Flight-Weather Lakehouse Stream Engine
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/scripts/run_pipeline.py
Restart=always
RestartSec=15
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=-$APP_DIR/.env

# Security Hardening
ProtectSystem=full
NoNewPrivileges=true
PrivateTmp=true

# Standard Journal Logging
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable flight-engine.service

echo "=== [6/7] Configuring Weekly Autonomous Archival Sentinel Cron Job ==="
# Runs every Sunday at 23:50 UTC to evict and compact aged partitions
CRON_JOB="50 23 * * 0 $APP_DIR/venv/bin/python -c 'from src.archival.eviction_sentinel import EvictionSentinel; from datetime import datetime, timezone, timedelta; target=(datetime.now(timezone.utc)-timedelta(days=7)).strftime(\"%Y-%m-%d\"); EvictionSentinel().evict_partition(\"silver_flight_weather_telemetry\", target)' >> /var/log/flight-archival.log 2>&1"

sudo tee /etc/cron.d/flight-archival-sentinel > /dev/null <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
$CRON_JOB
EOF
sudo chmod 644 /etc/cron.d/flight-archival-sentinel

echo "=== [7/7] Hardening Network Firewall ==="
sudo ufw allow 22/tcp comment 'Allow SSH'
sudo ufw --force enable

echo "=========================================================================="
echo "✨ VM SETUP COMPLETE: Eco-Friendly Hybrid Flight-Weather Engine Installed"
echo "To start the background engine:"
echo "    sudo systemctl start flight-engine.service"
echo "To check live status:"
echo "    sudo systemctl status flight-engine.service"
echo "To tail structured stream logs:"
echo "    journalctl -u flight-engine.service -f -o cat"
echo "=========================================================================="

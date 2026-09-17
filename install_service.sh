#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SERVICE_NAME="binance-prediction-scanner"
SERVICE_USER="${SUDO_USER:-$(id -un)}"
VENV_DIR="$REPO_DIR/.venv"

if [[ ! -f "$REPO_DIR/scanner.py" ]]; then
  echo "Run this script from the cloned project directory." >&2
  exit 1
fi

sudo apt-get update
sudo apt-get install -y python3 python3-venv

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/python" -m pip install -r "$REPO_DIR/requirements.txt"

if [[ ! -f "$REPO_DIR/.env" ]]; then
  cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
  chmod 600 "$REPO_DIR/.env"
  echo "Created $REPO_DIR/.env. Fill in credentials and NTFY_TOPIC, then rerun this script."
  exit 0
fi

chmod 600 "$REPO_DIR/.env"

sudo tee "/etc/systemd/system/$SERVICE_NAME.service" >/dev/null <<EOF
[Unit]
Description=Binance prediction market scanner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_DIR
ExecStart=$VENV_DIR/bin/python $REPO_DIR/scanner.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1
NoNewPrivileges=true
ProtectSystem=full
ReadWritePaths=$REPO_DIR

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"
sudo systemctl --no-pager --full status "$SERVICE_NAME" || true
echo "Use: sudo journalctl -u $SERVICE_NAME -f"

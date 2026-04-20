#!/bin/bash -e
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

(sudo systemctl status node_exporter && exit 0) || echo "Installing node explorer"
NODE_EXPORTER_BASE=https://github.com/prometheus/node_exporter/releases/download/v0.18.1
curl -LO "$NODE_EXPORTER_BASE/node_exporter-0.18.1.linux-amd64.tar.gz"
tar -xvf node_exporter-0.18.1.linux-amd64.tar.gz
sudo mv node_exporter-0.18.1.linux-amd64/node_exporter /usr/local/bin/
sudo useradd -rs /bin/false node_exporter || true
echo "[Unit]
Description=Node Exporter
After=network.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter --web.listen-address=:9200

[Install]
WantedBy=multi-user.target
" > node_exporter.service
sudo mv node_exporter.service /etc/systemd/system/node_exporter.service
sudo systemctl daemon-reload
sudo systemctl start node_exporter
sudo systemctl enable node_exporter

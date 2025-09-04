#!/bin/bash

echo "Restarting vivarium service..."

sudo systemctl stop vivarium.service
sudo systemctl disable vivarium.service
sudo systemctl daemon-reload
sudo systemctl enable vivarium.service
sudo systemctl start vivarium.service
sudo journalctl -u vivarium.service -f
#!/bin/bash
echo "Updating vivarium service..."
cd /home/calcasmy/vivarium || exit 1

git fetch origin main && git reset --hard origin/main
./restart_vivarium_service.sh
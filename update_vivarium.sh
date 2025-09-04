#!/bin/bash
echo "Updating vivarium service..."
cd vivarium || exit 1

git fetch origin main && git reset --hard origin/main

cd ..
./restart_vivarium_service.sh
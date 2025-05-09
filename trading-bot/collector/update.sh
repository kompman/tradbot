#!/bin/bash
cd "$(dirname "$0")"

echo "Updating tradbot…"
git pull origin main
go mod tidy
go build -o tradbot .

echo "Restarting tradbot…"
pkill tradbot
nohup ./tradbot > bot.log 2>&1 &

echo "Done."

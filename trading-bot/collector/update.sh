#!/bin/bash
git pull origin main
go mod tidy
go build -o tradbot .
# если systemd:
sudo systemctl restart tradbot

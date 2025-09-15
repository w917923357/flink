#!/bin/bash

echo "Starting socket server on port 9999..."
echo "========================================="

# Check if the port is already in use
if lsof -Pi :9999 -sTCP:LISTEN -t >/dev/null ; then
    echo "Error: Port 9999 is already in use"
    exit 1
fi

nc -l -k 9999

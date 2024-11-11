#!/bin/bash

# List of VM IPs
vmIPs=(
  "172.22.94.178"
  "172.22.156.179"
  "172.22.158.179"
  "172.22.94.179"
  "172.22.156.180"
  "172.22.158.180"
  "172.22.94.180"
  "172.22.156.181"
  "172.22.158.181"
  "172.22.94.181"
)

# Loop through each VM IP and stop Go processes
for vmIP in "${vmIPs[@]}"; do
  echo "Connecting to $vmIP and stopping Go processes..."
  ssh -o "StrictHostKeyChecking no" allent3@$vmIP 'ps aux | grep "[g]o" | awk "{print \$2}" | xargs -r kill -9'
  echo "Go processes stopped on $vmIP"
done

echo "Completed stopping Go processes on all VMs."

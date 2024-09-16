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

LOG_FILE="tests.log"           # The log file to deploy
REMOTE_DIR="~/go_project/logs" # Destination directory on the VM

# Check if the log file exists
if [ ! -f "$LOG_FILE" ]; then
  echo "Log file $LOG_FILE not found!"
  exit 1
fi

# Function to copy log file to each VM
deploy_log() {
  local ip=$1
  echo "Deploying log file to $ip..."
  ssh "allent3@$ip" "mkdir -p $REMOTE_DIR"  # Ensure the remote directory exists
  scp "$LOG_FILE" "allent3@$ip:$REMOTE_DIR" # Copy the log file
  echo "Log file deployed to $ip"
}

# Main loop to deploy the log file to all VMs
for ip in "${vmIPs[@]}"; do
  echo "Processing $ip..."

  deploy_log "$ip"
  echo "Log file deployment completed for $ip"

  echo "Finished processing $ip"
  echo "------------------------"
done

echo "Log file deployment completed on all VMs."

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
REPO_DIR="."                    # Replace with your local repo path
REMOTE_DIR="~/go_project"       # Destination folder on the VM
SERVER_DIR="$REMOTE_DIR/server" # Path to server directory on the VM
LOG_FILE="~/go_server.log"      # Log file for the Go server

for ip in "${vmIPs[@]}"; do
  echo "Deleting existing repo from $ip"
  ssh -n "allent3@$ip" "rm -rf go_project/"

  echo "Copying repo to $ip..."
  scp -r "$REPO_DIR" "allent3@$ip:$REMOTE_DIR"

  if [ $? -eq 0 ]; then
    echo "Successfully propagated repo to $ip"
    echo "Starting server on $ip..."
    ssh -n "allent3@$ip" "cd $SERVER_DIR && go build -o server && nohup ./server > $LOG_FILE 2>&1 &"

    if [ $? -eq 0 ]; then
      echo "Server started successfully on $ip"
      echo "You can check the log file at $LOG_FILE on $ip"
    else
      echo "Failed to start server on $ip"
      echo "Last few lines of the log file:"
      ssh -n "allent3@$ip" "tail -n 20 $LOG_FILE"
    fi
  else
    echo "Failed to propagate repo to $ip"
  fi
done

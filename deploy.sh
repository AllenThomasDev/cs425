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

REPO_DIR="."                    # Local repo path
REMOTE_DIR="~/go_project"       # Destination folder on the VM
SERVER_DIR="$REMOTE_DIR/server" # Path to server directory on the VM

# Function to copy files
copy_files() {
  local ip=$1
  echo "Copying files to $ip..."
  ssh "allent3@$ip" "rm -rf $REMOTE_DIR && mkdir -p $REMOTE_DIR"
  scp -r "$REPO_DIR" "allent3@$ip:$REMOTE_DIR"
  echo "Files copied to $ip"
}

# Function to start server
start_server() {
  local ip=$1
  echo "Starting server on $ip..."
  ssh -n "allent3@$ip" "cd $SERVER_DIR && nohup go run main.go > ~/go_server.log 2>&1 &" &
  echo "Server start command sent to $ip"
}

# Main loop
for ip in "${vmIPs[@]}"; do
  echo "Processing $ip..."

  copy_files "$ip"
  echo "File copy completed for $ip"

  start_server "$ip"
  echo "Server start command completed for $ip"

  echo "Finished processing $ip"
  echo "------------------------"
done

echo "Script execution completed"

#!/bin/bash

if [ $# -eq 0 ]; then
  echo "No argument supplied. Please provide a net-id."
  exit 1
fi

# List of VM IPs
VMDomainNames=(
  "fa24-cs425-5401.cs.illinois.edu"
  "fa24-cs425-5402.cs.illinois.edu"
  # "fa24-cs425-5403.cs.illinois.edu"
  # "fa24-cs425-5404.cs.illinois.edu"
  # "fa24-cs425-5405.cs.illinois.edu"
  # "fa24-cs425-5406.cs.illinois.edu"
  # "fa24-cs425-5407.cs.illinois.edu"
  # "fa24-cs425-5408.cs.illinois.edu"
  # "fa24-cs425-5409.cs.illinois.edu"
  # "fa24-cs425-5410.cs.illinois.edu"
)

REPO_DIR="."                    # Local repo path
REMOTE_DIR="~/go_project"       # Destination folder on the VM
SERVER_DIR="$REMOTE_DIR/server" # Path to server directory on the VM
USER=$1                         # User to ssh as

# Function to copy files
copy_files() {
  local domain_name=$1
  echo "Copying files to $domain_name..."
  ssh "$USER@$domain_name" "rm -rf $REMOTE_DIR && mkdir -p $REMOTE_DIR"
  rsync -av --exclude='.*' "$REPO_DIR/" "$USER@$domain_name:$REMOTE_DIR"
  echo "Files copied to $domain_name"
}

# Function to start server
start_server() {
  local domain_name=$1
  echo "Starting server on $domain_name..."
  ssh -n "$USER@$domain_name" "cd $SERVER_DIR && nohup go run main.go > ~/go_server.log 2>&1 &" &
  echo "Server start command sent to $domain_name"
}

# Main loop
for domain_name in "${VMDomainNames[@]}"; do
  echo "Processing $domain_name..."

  copy_files "$domain_name"
  echo "File copy completed for $domain_name"

  start_server "$domain_name"
  echo "Server start command completed for $domain_name"

  echo "Finished processing $domain_name"
  echo "------------------------"
done

echo "Script execution completed"

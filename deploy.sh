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
# Directory of your Go project on your local machine
REPO_DIR="."                    # Replace with your local repo path
REMOTE_DIR="~/go_project"       # Destination folder on the VM
CLIENT_DIR="$REMOTE_DIR/client" # Path to client directory on the VM

for ip in "${vmIPs[@]}"; do
  echo "deleting existing repo from $ip"
  ssh -n "allent3@$ip" "rm -rf go_project/"
  echo "copying repo to $ip..."
  scp -r "$REPO_DIR" "allent3@$ip:$REMOTE_DIR"
  if [ $? -eq 0 ]; then
    echo "successfully propagated repo to $ip"

    echo "starting client on $ip..."
    ssh -n "allent3@$ip" "cd $CLIENT_DIR && nohup go run main.go > /dev/null 2>&1 &"

    if [ $? -eq 0 ]; then
      echo "client started successfully on $ip"
    else
      echo "failed to start client on $ip"
    fi
  else
    echo "failed to propagate repo to $ip"
  fi
done

#!/bin/bash

if [ $# -eq 0 ]; then
  echo "No argument supplied. Please provide a net-id."
  exit 1
fi

VMDomainNames=(
  "fa24-cs425-5401.cs.illinois.edu"
  "fa24-cs425-5402.cs.illinois.edu"
  "fa24-cs425-5403.cs.illinois.edu"
  "fa24-cs425-5404.cs.illinois.edu"
  "fa24-cs425-5405.cs.illinois.edu"
  "fa24-cs425-5406.cs.illinois.edu"
  "fa24-cs425-5407.cs.illinois.edu"
  "fa24-cs425-5408.cs.illinois.edu"
  "fa24-cs425-5409.cs.illinois.edu"
  "fa24-cs425-5410.cs.illinois.edu"
)

REPO_DIR="../mp3/"
EXECUTABLE="daemon"
REMOTE_DIR="~"
USER=$1

# Build the Go executable locally
echo "Building Go executable..."
cd "$REPO_DIR" || exit
env GOOS=linux GOARCH=amd64 go build -o "$EXECUTABLE" ./ || {
  echo "Go build failed"
  exit 1
}
echo "Build completed. Executable: $EXECUTABLE"

# Function to copy the executable to a remote server
copy_executable() {
  local domain_name=$1
  echo "Copying executable to $domain_name..."
  scp "$EXECUTABLE" "$USER@$domain_name:$REMOTE_DIR"
  echo "Executable copied to $domain_name"
}

# Iterate over each VM and copy the executable
for domain_name in "${VMDomainNames[@]}"; do
  echo "Processing $domain_name..."
  ssh "$USER@$domain_name" "mkdir -p $REMOTE_DIR"
  copy_executable "$domain_name"
  echo "Finished processing $domain_name"
  echo "------------------------"
done

echo "Script execution completed"

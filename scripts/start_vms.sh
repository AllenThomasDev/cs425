#!/bin/bash
if [ $# -eq 0 ]; then
  echo "No argument supplied. Please provide a net-id."
  exit 1
fi

# List of VM SSH addresses
vm_hosts=(
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

# Start a new tmux session named 'vm-session' and log into the first VM
tmux new-session -d -s vm-session "ssh $1@${vm_hosts[0]}"

# Loop through the remaining VM hosts and create new panes
for host in "${vm_hosts[@]:1}"; do
  tmux split-window -h "ssh $host"
  tmux select-layout tiled
done

# Attach to the tmux session
tmux attach -t vm-session

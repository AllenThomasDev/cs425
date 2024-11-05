#!/bin/bash

# List of VM SSH addresses
vm_hosts=(
  "allent3@fa24-cs425-5401.cs.illinois.edu"
  "allent3@fa24-cs425-5402.cs.illinois.edu"
  "allent3@fa24-cs425-5403.cs.illinois.edu"
  "allent3@fa24-cs425-5404.cs.illinois.edu"
  "allent3@fa24-cs425-5405.cs.illinois.edu"
  "allent3@fa24-cs425-5406.cs.illinois.edu"
  "allent3@fa24-cs425-5407.cs.illinois.edu"
  "allent3@fa24-cs425-5408.cs.illinois.edu"
  "allent3@fa24-cs425-5409.cs.illinois.edu"
  "allent3@fa24-cs425-5410.cs.illinois.edu"
)

# Start a new tmux session named 'vm-session' and log into the first VM
# tmux new-session -d -s vm-session "ssh ${vm_hosts[0]}"

# Loop through the remaining VM hosts and create new panes
for host in "${vm_hosts[@]:0}"; do
  tmux split-window -h "ssh $host"
  tmux select-layout tiled
done

# Attach to the tmux session
# tmux attach -t vm-session

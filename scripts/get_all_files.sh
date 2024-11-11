#!/bin/bash

# Define the target pane (adjust this to your session, window, and pane numbers)
target_pane="0:1.1"
target_window="0:1"

# Loop through each file from b1.txt to b100.txt, three times
for repeat in {1..3}; do
  for i in $(seq 1 100); do
    # Format filenames based on the current index
    tmux set-window-option -t "$target_window" synchronize-panes off
    file="b${i}.txt"
    local_file="local_b${i}_run${repeat}.txt"

    # Send the get command with both arguments to the specified tmux pane
    tmux send-keys -t "$target_pane" "get ${file} ${local_file}" Enter

    # Wait for half a second
    sleep 0.5
  done
done

#!/bin/bash

# Define the target pane (adjust this to your session, window, and pane numbers)
target_pane="0:1.1"
target_window="0:1"
# Loop through files 1 to 100
for i in $(seq 1 100); do
  # Format filenames based on the current index
  tmux set-window-option -t "$target_window" synchronize-panes off
  src_file="business_${i}.txt"
  dest_file="b${i}.txt"

  # Send the command to the specified tmux pane
  tmux send-keys -t "$target_pane" "create ${src_file} ${dest_file}" Enter

  # Wait for half a second
  sleep 0.5
done

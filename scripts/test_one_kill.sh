#!/bin/bash

# kill two distinct non-source nodes 
# panes 4-9 are ops
rand1=$((4 + $RANDOM % 6))
rand2=$((4 + $RANDOM % 6))
while [[ $rand1 == $rand2 ]]; do
    rand2=$((4 + $RANDOM % 6))
done
echo $rand1
echo $rand2
tmux select-pane -t $rand1
tmux send-keys C-c
tmux select-pane -t $rand2
tmux send-keys C-c
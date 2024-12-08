#!/bin/bash

# kill a stateful task and another distinct task
# panes 7-9 are stateful
rand1=$((7 + $RANDOM % 3))
# kill another non-source task
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
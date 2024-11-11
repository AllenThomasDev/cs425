#!/bin/bash


halfArgs=$(("$#"/2))
if [ $(($halfArgs*2)) == $# ]; then
    echo "Usage: filename VMi VMj ... localfilenamei localfilenamej"
else
    for i in $(seq 1 $halfArgs);
    do
        vm_index=$(($i+1))
        file_index=$(($vm_index+$halfArgs))
        vm_list[$i]=${!vm_index}
        file_list[$i]=${!file_index}
    done

    for i in $(seq 1 $halfArgs);
    do
        tmux select-pane -t ${vm_list[$i]}
        tmux send-keys "append ${file_list[$i]} $1" Enter
    done
fi

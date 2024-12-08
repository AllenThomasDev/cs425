# cs425-mp3

### send the code and sample executables to VMs

1. `chmod +x deploy.sh`
2. `./deploy.sh <net-id>`
3. `chmod +x deploy_execs.sh`
4. `./deploy_execs.sh <net-id>`

### Running the Client

1. `./start_vms.sh <net-id>`
2. This will open 10 tmux panes, from where you can start interacting with the daemon
3. ./daemon to start the daemon on the VM
4. RainStorm <op1_exe> <op1_type> <op1_arg> <op2_exe> <op2_type> <op2_arg> <HyDFS_src> <HyDFS_dest> <num_tasks> to run RainStorm

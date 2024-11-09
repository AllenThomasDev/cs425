import random
import time
import subprocess

# Define parameters
total_operations = 300
file_count = 100  # Total number of files (b1.txt to b100.txt)
target_pane = "0:1.2"

# Function to run a tmux command
def run_tmux_command(command):
    subprocess.run(["tmux", "send-keys", "-t", target_pane, command, "Enter"])

# Perform operations
for _ in range(total_operations):
    # Uniformly select a file index between 1 and file_count
    file_index = random.randint(1, file_count)
    hydfs_file = f"b{file_index}.txt"
    local_get_file = f"local_append_get_{file_index}.txt"
    local_append_file = f"business_{file_index}.txt"
    
    # Randomly decide between 'get' (90%) and 'append' (10%)
    if random.random() < 0.9:
        # Perform 'get' operation, writing to a unique local file
        run_tmux_command(f"get {hydfs_file} {local_get_file}")
    else:
        # Perform 'append' operation from the business file
        run_tmux_command(f"append {local_append_file} {hydfs_file}")
    
    # Wait briefly to simulate realistic operation intervals
    time.sleep(0.5)

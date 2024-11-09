import random
import time
import subprocess
import numpy as np

# Define parameters
total_operations = 300 
file_count = 100  # Total number of files (b1.txt to b100.txt)
target_pane = "0:1.2"
zipf_param = 1.5  # Zipfian distribution parameter; adjust to control skew

# Generate Zipfian-distributed file indices, limited to file_count range
zipf_indices = np.random.zipf(zipf_param, total_operations)
zipf_indices = [min(i, file_count) for i in zipf_indices]

# Function to run a tmux command
def run_tmux_command(command):
    subprocess.run(["tmux", "send-keys", "-t", target_pane, command, "Enter"])

# Perform operations
for i in range(total_operations):
    # Select file index based on Zipfian distribution
    file_index = zipf_indices[i]
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

import random
import time
import subprocess

# Define the target pane (adjust this to your session, window, and pane numbers)
target_pane = "0:1.1"
target_window = "0:1"

# Run the loop 300 times with a random file index each time
for j in range(300):
    # Generate a random number between 1 and 100
    i = random.randint(1, 100)
    print(j) # Format filenames based on the random index
    file = f"b{i}.txt"
    local_file = f"local_b{i}_run_random.txt"

    # Turn off synchronize-panes
    subprocess.run(["tmux", "set-window-option", "-t", target_window, "synchronize-panes", "off"])

    # Send the get command with both arguments to the specified tmux pane
    command = f"get {file} {local_file}"
    subprocess.run(["tmux", "send-keys", "-t", target_pane, command, "Enter"])

    # Wait for half a second
    time.sleep(0.5)

# Define the target pane (adjust this to your session, window, and pane numbers)
target_pane="0:1.1"
target_window="0:1"

# Generate Zipfian-distributed indices for 100 files using a more controlled approach
zipf_indices=$(python3 -c '
import numpy as np

def generate_zipfian_indices(num_files, num_requests, s=1.5):
    # Generate raw Zipfian distribution
    x = np.arange(1, num_files + 1)
    # Calculate probabilities for each file
    probs = 1 / (x ** s)
    probs = probs / probs.sum()
    
    # Generate indices based on these probabilities
    indices = np.random.choice(
        np.arange(1, num_files + 1),
        size=num_requests,
        p=probs
    )
    return indices

# Generate 1000 requests for 100 files
indices = generate_zipfian_indices(100, 300)
print(" ".join(map(str, indices.astype(int))))
')

# Validate that we received indices
if [ -z "$zipf_indices" ]; then
  echo "Failed to generate Zipfian indices"
  exit 1
fi

# Disable pane synchronization
tmux set -t "$target_window" synchronize-panes off

# Loop through the generated Zipfian indices
for i in $zipf_indices; do
  # Validate index is within expected range
  if [ "$i" -ge 1 ] && [ "$i" -le 100 ]; then
    # Format filenames based on the current index
    hydfs_file="b${i}.txt"
    local_file="local_b${i}_zipfian.txt"

    # Send the get command with both arguments to the specified tmux pane
    tmux send-keys -t "$target_pane" "get ${hydfs_file} ${local_file}" Enter

    # Add a small delay to prevent overwhelming the system
    sleep 0.5
  else
    echo "Warning: Invalid index generated: $i"
  fi
done

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_and_process_csv(filename):
    # Read CSV file with custom settings
    df = pd.read_csv(filename, 
                     header=None, 
                     names=['Time', 'Unit', 'Cache Used'],
                     skipinitialspace=True,
                     dtype=str)
    
    # Clean up and convert columns
    df['Time'] = pd.to_numeric(df['Time'])
    df['Unit'] = df['Unit'].str.strip()
    df['Cache Used'] = df['Cache Used'].str.strip().str.lower() == 'true'
    
    # Convert time units to milliseconds
    df['Time (ms)'] = df.apply(lambda row: row['Time'] / 1000 if row['Unit'] == 'µs' else row['Time'], axis=1)
    
    return df

# Define file paths for all scenarios
files = {
    "Random No Cache": "./random/csvs/random-only-latency-no-cache.csv",
    "Random 50% Cache": "./random/csvs/random-only-latency.csv",
    "Zipfian No Cache": "./zipfian/csvs/zipf-only-latency-no-cache.csv",
    "Zipfian 50% Cache": "./zipfian/csvs/zipf-only-latency.csv",
}

# Load all dataframes
dataframes = {name: load_and_process_csv(file) for name, file in files.items()}

# Calculate statistics
stats = {}
for name, df in dataframes.items():
    mean = df['Time (ms)'].mean()
    std_dev = df['Time (ms)'].std()
    des = df['Time (ms)'].describe()
    stats[name] = {"mean": mean, "std_dev": std_dev, "des": des}

# Convert stats to DataFrame
stats_df = pd.DataFrame(stats).T

# Create two separate dataframes for Random and Zipfian
random_df = stats_df[[name.startswith('Random') for name in stats_df.index]]
zipfian_df = stats_df[[name.startswith('Zipfian') for name in stats_df.index]]

# ... (keep all the code before plotting the same)
# Plotting
plt.figure(figsize=(15, 10))  # Increased height further

# Create bar positions
cache_sizes = ['No Cache','50%']
x = np.arange(len(cache_sizes))
width = 0.35

# Plot Random distribution bars
random_bars = plt.bar(x - width/2, random_df['mean'], width, yerr=random_df['std_dev'], 
       label='Random Distribution', color='skyblue', capsize=5, ecolor='grey')  # Changed error bar color

# Plot Zipfian distribution bars
zipfian_bars = plt.bar(x + width/2, zipfian_df['mean'], width, yerr=zipfian_df['std_dev'],
       label='Zipfian Distribution', color='lightcoral', capsize=5, ecolor='grey')  # Changed error bar color

# Add value labels on the bars with more padding and full precision
def autolabel(rects, y_offset=0.15):
    for rect in rects:
        height = rect.get_height()
        # Adjust label position by adding y_offset and adding bbox for background
        plt.text(
            rect.get_x() + rect.get_width() / 2.,
            height,
            f'{height:.6f} ms',
            ha='center', va='bottom', fontsize=10,
            color='black',bbox=dict(facecolor='white', edgecolor='none', pad=0.3, alpha=0.7)
        )

autolabel(random_bars)
autolabel(zipfian_bars)

# Customize plot
plt.xlabel('Cache Size', fontsize=12)
plt.ylabel('Latency (ms)', fontsize=12)
plt.title('Latency Comparison: Random vs Zipfian Distribution with Different Cache Sizes', fontsize=14, pad=20)
plt.xticks(x, cache_sizes, fontsize=10)
plt.legend(fontsize=10)

# Add grid for better readability
plt.grid(True, axis='y', linestyle='--', alpha=0.7)

# Adjust y-axis limits to accommodate labels
plt.margins(y=0.3)  # Increased top margin to 30%

plt.tight_layout()
plt.show()
print("\nDetailed Statistics:")
print("\nRandom Distribution:")
for idx, row in random_df.iterrows():
    print(f"{idx}: Mean = {row['mean']:.3f}ms, Std Dev = {row['std_dev']:.3f}ms {row['des']}")

print("\nZipfian Distribution:")
for idx, row in zipfian_df.iterrows():
    print(f"{idx}: Mean = {row['mean']:.3f}ms, Std Dev = {row['std_dev']:.3f}ms {row['des']}")

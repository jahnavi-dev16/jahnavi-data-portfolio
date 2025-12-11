
# Step 3: Analyze partitions safely on Windows

import pandas as pd
import os

# Read and combine all partitioned files manually using Pandas
data_frames = []
for root, dirs, files in os.walk("partitioned_data_month"):
    for file in files:
        if file.endswith(".csv"):
            path = os.path.join(root, file)
            df = pd.read_csv(path)
            data_frames.append(df)

# Combine all data
df_combined = pd.concat(data_frames, ignore_index=True)

# Show top 5 rows
print("\nSample data:")
print(df_combined.head())

# Show total records
print("\nTotal records:", len(df_combined))

# Show how many partitions (month folders) exist
partitions = [d for d in os.listdir("partitioned_data_month") if os.path.isdir(os.path.join("partitioned_data_month", d))]
print("\nPartitions found:", len(partitions))
print("Folders:", partitions)

# Show where each partition is stored
print("\nData stored in folders:")
for p in partitions:
    print(f" → partitioned_data_month/{p}")

print("\n Analysis complete! Data successfully verified from all partitions.")
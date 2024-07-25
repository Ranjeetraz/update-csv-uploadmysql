

import os
import pandas as pd
import glob

# Directory containing the CSV files
csv_directory = r"C:\Users\IHQ-All-csv"

# Step 1: Read all CSV files into a list of DataFrames
all_csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
dfs = [pd.read_csv(file) for file in all_csv_files]

# Step 2: Assign auto-incremented IDs to each row in each DataFrame
current_id = 1
old_to_new_id = {}

for df in dfs:
    num_rows = len(df)
    new_ids = list(range(current_id, current_id + num_rows))
    
    # Assign new IDs to the 'id' column and create a mapping of old to new IDs
    if 'id' in df.columns:
        old_ids = df['id'].tolist()
        for old_id, new_id in zip(old_ids, new_ids):
            old_to_new_id[old_id] = new_id
          
        df['id'] = new_ids
    else:
        df['id'] = new_ids
    
    current_id += num_rows

# Step 3: Replace old IDs with new auto-incremented IDs in all columns
for df in dfs:
    for col in df.columns:
        if col != 'id':
            df[col] = df[col].apply(lambda x: old_to_new_id.get(x, x))

# Step 4: Save updated DataFrames back to CSV files
for i, df in enumerate(dfs):
    file_path = all_csv_files[i]
    df.to_csv(file_path, index=False)

print("Auto-incremented IDs assigned and CSV files saved successfully.")

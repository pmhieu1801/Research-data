import os
import glob
import pandas as pd

# Find all CSV files in the current directory
csv_files = glob.glob('*.csv')

if not csv_files:
    print('No CSV files found.')
    exit(1)

# Merge all CSV files
merged_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Save to merged.csv
merged_df.to_csv('merged.csv', index=False, encoding='utf-8-sig')
print(f'Merged {len(csv_files)} files into merged.csv')

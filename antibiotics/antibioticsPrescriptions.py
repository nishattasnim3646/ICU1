import pandas as pd
import time
import psutil
import os

start_time = time.time()

input_path = '/home/nishat/physionet.org/files/mimiciv/3.1/hosp/prescriptions.csv'
output_path = '/home/nishat/physionet.org/files/mimiciv/3.1/ICU1/antibiotics/prescriptions_filtered.csv'

# Target drugs (case-insensitive)
target_drugs = ['vancomycin', 'cefepime', 'meropenem', 'cefazolin', 'piperacillin']

print("Starting chunk processing...")
chunksize = 50000  # Process 50k rows at a time
required_cols = ['subject_id', 'hadm_id', 'starttime', 'stoptime', 'drug']

first_chunk = True
total_rows_processed = 0
total_rows_filtered = 0
chunk_num = 0

try:
    for chunk in pd.read_csv(input_path, usecols=required_cols, chunksize=chunksize):
        chunk_num += 1
        total_rows_processed += len(chunk)
        
        # Convert to lowercase for case-insensitive matching
        chunk['drug_lower'] = chunk['drug'].astype(str).str.lower()
        
        # Create filter mask
        mask = chunk['drug_lower'].str.contains('|'.join(target_drugs), na=False)
        filtered_chunk = chunk[mask].copy()
        
        # Remove the temporary column
        filtered_chunk = filtered_chunk[required_cols]
        
        total_rows_filtered += len(filtered_chunk)
        
        # Write to file
        if first_chunk:
            filtered_chunk.to_csv(output_path, index=False)
            first_chunk = False
        else:
            filtered_chunk.to_csv(output_path, mode='a', header=False, index=False)
        
        # Progress update every 10 chunks
        if chunk_num % 10 == 0:
            # Get memory usage
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            print(f"Chunk {chunk_num}: Processed {total_rows_processed:,} rows, filtered {total_rows_filtered:,} rows")
            print(f"  Memory usage: {memory_mb:.1f} MB")
        
        # Clear memory
        del chunk, filtered_chunk, mask
        
except Exception as e:
    print(f"Error occurred: {e}")
    print(f"Last processed chunk: {chunk_num}")
    print(f"Total rows processed before error: {total_rows_processed:,}")

print(f"\nTotal processing time: {time.time() - start_time:.2f} seconds")
print(f"Total rows processed: {total_rows_processed:,}")
print(f"Total rows filtered: {total_rows_filtered:,}")

# Show summary if file was created
if os.path.exists(output_path):
    try:
        # Count lines in output file
        with open(output_path, 'r') as f:
            line_count = sum(1 for line in f)
        print(f"Lines in output file: {line_count:,} (includes header)")
        
        # Show first few rows
        print("\nFirst 5 rows of filtered data:")
        result_df = pd.read_csv(output_path, nrows=5)
        print(result_df)
    except:
        print("Could not read output file for summary")
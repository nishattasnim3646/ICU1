import pandas as pd
import numpy as np

# Read the CSV files
final_df = pd.read_csv('final.csv')
abx_df = pd.read_csv('antibiotic_resistance_events.csv')

print(f"final.csv rows: {len(final_df)}")
print(f"antibiotic_resistance_events.csv rows: {len(abx_df)}")
print(f"Common subject_ids: 19,373")

# Clean the data
# Convert to string and strip whitespace
final_df['subject_id'] = final_df['subject_id'].astype(str).str.strip()
final_df['hadm_id'] = final_df['hadm_id'].astype(str).str.strip()

abx_df['subject_id'] = abx_df['subject_id'].astype(str).str.strip()
abx_df['hadm_id'] = abx_df['hadm_id'].astype(str).str.strip()

# Replace empty strings with NaN
abx_df['hadm_id'] = abx_df['hadm_id'].replace('', np.nan)
abx_df['hadm_id'] = abx_df['hadm_id'].replace('nan', np.nan)

# Create a copy of abx_df for matching
abx_with_hadm = abx_df[abx_df['hadm_id'].notnull()].copy()
abx_without_hadm = abx_df[abx_df['hadm_id'].isnull()].copy()

print(f"\nAntibiotic rows with hadm_id: {len(abx_with_hadm)}")
print(f"Antibiotic rows without hadm_id: {len(abx_without_hadm)}")

# OPTION 1: Create multiple rows (one per antibiotic event)
print("\n=== OPTION 1: Creating multiple rows (one per antibiotic event) ===")

merged_rows = []
abx_columns = [col for col in abx_df.columns if col not in ['subject_id', 'hadm_id']]

# Track progress
total_rows = len(final_df)
for idx, final_row in enumerate(final_df.itertuples(), 1):
    if idx % 1000 == 0:
        print(f"Processing row {idx}/{total_rows}")
    
    subject_id = final_row.subject_id
    hadm_id = final_row.hadm_id
    
    # Try to match with hadm_id first (more specific)
    matched_abx = abx_with_hadm[
        (abx_with_hadm['subject_id'] == subject_id) & 
        (abx_with_hadm['hadm_id'] == hadm_id)
    ]
    
    # If no match with hadm_id, try matching without hadm_id
    if len(matched_abx) == 0:
        matched_abx = abx_without_hadm[
            abx_without_hadm['subject_id'] == subject_id
        ]
    
    # Convert final_row to dictionary
    final_dict = final_row._asdict()
    # Remove Index from namedtuple
    if 'Index' in final_dict:
        del final_dict['Index']
    
    if len(matched_abx) > 0:
        # Add each antibiotic event as a separate row
        for _, abx_row in matched_abx.iterrows():
            merged_row = final_dict.copy()
            for col in abx_columns:
                merged_row[col] = abx_row[col]
            merged_rows.append(merged_row)
    else:
        # No antibiotic events found
        merged_row = final_dict.copy()
        for col in abx_columns:
            merged_row[col] = None
        merged_rows.append(merged_row)

# Create DataFrame
merged_df_option1 = pd.DataFrame(merged_rows)
print(f"\nOption 1 - Merged rows: {len(merged_df_option1)}")
print(f"  (original final.csv: {len(final_df)} rows)")
print(f"  Antibiotic events added for {len(merged_df_option1) - len(final_df)} additional rows")

# Save Option 1
merged_df_option1.to_csv('merged_final_with_antibiotics_multiple_rows.csv', index=False)
print("Saved Option 1 to: merged_final_with_antibiotics_multiple_rows.csv")

# OPTION 2: Aggregate antibiotic data into single row per patient
print("\n=== OPTION 2: Aggregating antibiotic data into single row ===")

def aggregate_abx_data(df):
    """Aggregate antibiotic data for a patient"""
    if len(df) == 0:
        return {col: None for col in abx_columns}
    
    aggregated = {}
    for col in abx_columns:
        if df[col].notnull().any():
            # For numeric columns, get unique values
            if col in ['dilution_value']:
                unique_vals = df[col].dropna().unique()
                aggregated[col] = '; '.join(str(v) for v in unique_vals)
            # For categorical/text columns
            else:
                unique_vals = df[col].dropna().unique()
                aggregated[col] = '; '.join(str(v) for v in unique_vals)
        else:
            aggregated[col] = None
    return aggregated

# Create a list for aggregated results
aggregated_rows = []

for idx, final_row in enumerate(final_df.itertuples(), 1):
    if idx % 1000 == 0:
        print(f"Processing row {idx}/{total_rows}")
    
    subject_id = final_row.subject_id
    hadm_id = final_row.hadm_id
    
    # Convert to dictionary
    final_dict = final_row._asdict()
    if 'Index' in final_dict:
        del final_dict['Index']
    
    # Find matching antibiotic events
    matched_with_hadm = abx_with_hadm[
        (abx_with_hadm['subject_id'] == subject_id) & 
        (abx_with_hadm['hadm_id'] == hadm_id)
    ]
    
    matched_without_hadm = abx_without_hadm[
        abx_without_hadm['subject_id'] == subject_id
    ]
    
    # Combine matches
    all_matches = pd.concat([matched_with_hadm, matched_without_hadm])
    
    # Aggregate antibiotic data
    abx_aggregated = aggregate_abx_data(all_matches)
    
    # Merge with final row
    merged_row = {**final_dict, **abx_aggregated}
    aggregated_rows.append(merged_row)

# Create DataFrame
merged_df_option2 = pd.DataFrame(aggregated_rows)

# Rename antibiotic columns to avoid confusion
rename_dict = {col: f'abx_{col}' for col in abx_columns}
merged_df_option2 = merged_df_option2.rename(columns=rename_dict)

print(f"\nOption 2 - Merged rows: {len(merged_df_option2)}")
print(f"Rows with antibiotic data: {merged_df_option2['abx_org_name'].notnull().sum()}")

# Save Option 2
merged_df_option2.to_csv('merged_final_with_antibiotics_aggregated.csv', index=False)
print("Saved Option 2 to: merged_final_with_antibiotics_aggregated.csv")

# Show a sample of merged data
print("\n=== SAMPLE OF MERGED DATA (Option 2) ===")
sample_cols = ['subject_id', 'hadm_id', 'abx_org_name', 'abx_ab_name', 'abx_interpretation']
sample_df = merged_df_option2[merged_df_option2['abx_org_name'].notnull()].head()

if len(sample_df) > 0:
    print(sample_df[sample_cols].to_string())
else:
    print("No antibiotic data found in sample")

print("\n=== SUMMARY ===")
print(f"Total patients in final.csv: {len(final_df)}")
print(f"Patients with antibiotic data (Option 2): {merged_df_option2['abx_org_name'].notnull().sum()}")
print(f"Multiple rows created (Option 1): {len(merged_df_option1)}")
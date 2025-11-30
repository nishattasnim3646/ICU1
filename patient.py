import pandas as pd
import os

data_path = "/home/nishat/physionet.org/files/mimiciv/3.1/"

print("=== CORRECTED FILTERING ===")

# Read data
icustays = pd.read_csv(data_path + "icu/icustays.csv")
print(f"Total ICU stays: {len(icustays)}")

# CORRECTED care unit names
adult_icus = [
    'Medical Intensive Care Unit (MICU)',
    'Surgical Intensive Care Unit (SICU)', 
    'Trauma SICU',
    'Coronary Care Unit (CCU)',
    'Cardiac Vascular Intensive Care Unit (CVICU)',
    'Neuro Surgical Intensive Care Unit (Neuro SICU)',
     'Medical/Surgical Intensive Care Unit (MICU/SICU)',
    'Trauma SICU (TSICU)'
]

print(f"Looking for care units: {adult_icus}")

# Apply filters
filtered_adult = icustays[icustays['first_careunit'].isin(adult_icus)]
print(f"Adult ICUs: {len(filtered_adult)}")

filtered_los = filtered_adult[filtered_adult['los'] >= 1.25]  # 30 hours
print(f"LOS >= 30h: {len(filtered_los)}")

# First stay per patient
first_stays = filtered_los.sort_values(['subject_id', 'intime']).groupby('subject_id').head(1)
print(f"First stays per patient: {len(first_stays)}")

# Save the results
if len(first_stays) > 0:
    print("\nSaving filtered patients...")
    first_stays.to_csv('filtered_patients_fixed.csv', index=False)
    
    # Also save just subject IDs
    subject_ids = first_stays['subject_id'].unique()
    with open('filtered_subject_ids.txt', 'w') as f:
        for sid in subject_ids:
            f.write(f"{sid}\n")
    
    print(f"✅ SUCCESS! Saved {len(first_stays)} patients")
    print("Files created: filtered_patients_fixed.csv, filtered_subject_ids.txt")
    
    # Show sample
    print("\nFirst 5 patients:")
    print(first_stays[['subject_id', 'first_careunit', 'los']].head())
else:
    print("❌ No patients found after filtering")
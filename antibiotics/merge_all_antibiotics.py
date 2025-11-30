# merge_patients_with_antibiotics.py
import pandas as pd

print("=== MERGING ICU PATIENTS WITH ANTIBIOTICS ===")

# Load subject_id and hadm_id from ICU/patients.csv
icu_patients = pd.read_csv('ICU/patients.csv')[['subject_id', 'hadm_id']]
print(f"ICU patients: {len(icu_patients)} rows")

# Load antibiotic files
icu_abx = pd.read_csv('icu_antibiotics.csv')
hosp_abx = pd.read_csv('hosp_antibiotic.csv')

print(f"ICU antibiotics: {len(icu_abx)} records")
print(f"Hospital antibiotics: {len(hosp_abx)} records")

# Combine both antibiotic files
all_abx = pd.concat([icu_abx, hosp_abx], ignore_index=True)

# Merge with ICU patients (left join to keep all 32628 patients)
final_data = icu_patients.merge(all_abx, on='subject_id', how='left')

print(f"Final data: {len(final_data)} rows (should be {len(icu_patients)})")

# Save
final_data.to_csv('patients_with_antibiotics.csv', index=False)
print("✅ Saved: patients_with_antibiotics.csv")

print(f"\n=== SUMMARY ===")
print(f"ICU patients: {len(icu_patients)}")
print(f"Patients with antibiotics: {final_data['antibiotic'].notna().sum()}")
print(f"Unique patients with antibiotics: {final_data[final_data['antibiotic'].notna()]['subject_id'].nunique()}")

print(f"\nFirst 5 records:")
print(final_data.head())
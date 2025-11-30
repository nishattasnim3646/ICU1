# merge_final_with_combined_antibiotics.py
import pandas as pd

print("=== MERGING FINAL.CSV WITH COMBINED ANTIBIOTICS ===")

# Load final.csv as base
final_base = pd.read_csv('ICU/final.csv')
print(f"Final base: {len(final_base)} patients")

# Load patients with antibiotics
patients_abx = pd.read_csv('patients_with_antibiotics.csv')

# Combine multiple antibiotics into single cell per patient
combined_abx = patients_abx.groupby('subject_id').agg({
    'antibiotic': lambda x: ', '.join(x.dropna().unique()),
    'source_file': lambda x: ', '.join(x.dropna().unique())
}).reset_index()

print(f"Patients with combined antibiotics: {len(combined_abx)}")

# Merge with final base (left join to keep all final.csv patients)
final_with_abx = final_base.merge(combined_abx, on='subject_id', how='left')

print(f"Final with antibiotics: {len(final_with_abx)} patients")

# Save
final_with_abx.to_csv('FINAL_WITH_COMBINED_ANTIBIOTICS.csv', index=False)
print("✅ Saved: FINAL_WITH_COMBINED_ANTIBIOTICS.csv")

print(f"\n=== SUMMARY ===")
print(f"Final base patients: {len(final_base)}")
print(f"Patients with antibiotics: {final_with_abx['antibiotic'].notna().sum()}")

print(f"\nSample patients with multiple antibiotics:")
sample_patients = final_with_abx[final_with_abx['antibiotic'].notna()].head(3)
for _, row in sample_patients.iterrows():
    print(f"Subject {row['subject_id']}: {row['antibiotic']}")

print(f"\nFirst 3 patients:")
print(final_with_abx[['subject_id', 'antibiotic', 'source_file']].head(3))
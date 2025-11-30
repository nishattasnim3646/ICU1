# general_features_with_diabetes_hadm.py
import pandas as pd

print("=== COMPLETE GENERAL FEATURES + DIABETES + HADM_ID EXTRACTION ===")

# 1. Load filtered patients - BASE
filtered = pd.read_csv('patients.csv')
our_patients = filtered['subject_id'].tolist()
print(f"Processing {len(our_patients)} patients...")

# 2. Start with filtered patients as base
result = filtered[['subject_id']].copy()

# ------------------------------
# 3. Gender & Age
# ------------------------------
print("Extracting gender and age...")
patients = pd.read_csv('hosp/patients.csv')
demo_data = patients[patients['subject_id'].isin(our_patients)][['subject_id', 'gender', 'anchor_age']]
demo_data = demo_data.rename(columns={'anchor_age': 'age_years'})
result = result.merge(demo_data, on='subject_id', how='left')

# ------------------------------
# 4. Ethnicity + HADM_ID (first admission)
# ------------------------------
print("Extracting ethnicity and hadm_id...")
admissions = pd.read_csv('hosp/admissions.csv')
first_adm = admissions.sort_values(['subject_id', 'admittime']).groupby('subject_id').first().reset_index()

# Ethnicity
ethnicity_data = first_adm[first_adm['subject_id'].isin(our_patients)][['subject_id', 'race']]
ethnicity_data = ethnicity_data.rename(columns={'race': 'ethnicity'})
result = result.merge(ethnicity_data, on='subject_id', how='left')

# HADM_ID
hadm_data = first_adm[first_adm['subject_id'].isin(our_patients)][['subject_id', 'hadm_id']]
result = result.merge(hadm_data, on='subject_id', how='left')

# Reorder columns: subject_id, hadm_id, rest...
cols = result.columns.tolist()
cols.insert(1, cols.pop(cols.index('hadm_id')))  # move hadm_id to second position
result = result[cols]

# ------------------------------
# 5. Height
# ------------------------------
print("Extracting height...")
height_data = []
chunks = pd.read_csv('icu/chartevents.csv', chunksize=100_000, usecols=['subject_id','itemid','valuenum'])
for i, chunk in enumerate(chunks):
    h_chunk = chunk[(chunk['subject_id'].isin(our_patients)) & (chunk['itemid'] == 226730)]
    if not h_chunk.empty:
        height_data.append(h_chunk)
    if i % 10 == 0:
        print(f"  Processed {i+1} chunks...")

if height_data:
    all_height = pd.concat(height_data, ignore_index=True)
    first_height = all_height.groupby('subject_id').first().reset_index()
    height_df = first_height[['subject_id','valuenum']].rename(columns={'valuenum':'height_cm'})
    result = result.merge(height_df, on='subject_id', how='left')

# ------------------------------
# 6. Weight
# ------------------------------
print("Extracting weight...")
weight_data = []
chunks = pd.read_csv('icu/chartevents.csv', chunksize=100_000, usecols=['subject_id','itemid','valuenum'])
for i, chunk in enumerate(chunks):
    w_chunk = chunk[(chunk['subject_id'].isin(our_patients)) & (chunk['itemid'] == 226512)]
    if not w_chunk.empty:
        weight_data.append(w_chunk)
    if i % 10 == 0:
        print(f"  Processed {i+1} chunks...")

if weight_data:
    all_weight = pd.concat(weight_data, ignore_index=True)
    first_weight = all_weight.groupby('subject_id').first().reset_index()
    weight_df = first_weight[['subject_id','valuenum']].rename(columns={'valuenum':'weight_kg'})
    result = result.merge(weight_df, on='subject_id', how='left')

# ------------------------------
# 7. Diabetes Mellitus
# ------------------------------
print("Extracting diabetes mellitus...")
diag_chunks = pd.read_csv('hosp/diagnoses_icd.csv', chunksize=500_000)
diabetes_rows = []

for i, chunk in enumerate(diag_chunks):
    chunk = chunk[chunk['subject_id'].isin(our_patients)]
    if chunk.empty:
        continue

    # ICD-9 diabetes 250.xx
    chunk['is_dm_icd9'] = ((chunk['icd_version'] == 9) & chunk['icd_code'].astype(str).str.startswith('250')).astype(int)

    # ICD-10 diabetes E08–E13
    chunk['is_dm_icd10'] = ((chunk['icd_version'] == 10) & chunk['icd_code'].astype(str).str.startswith(('E08','E09','E10','E11','E13'))).astype(int)

    chunk['diabetes_mellitus'] = chunk[['is_dm_icd9','is_dm_icd10']].max(axis=1)
    diabetes_rows.append(chunk[['subject_id','diabetes_mellitus']])

    if i % 10 == 0:
        print(f"  Processed {i+1} chunks...")

if diabetes_rows:
    all_diabetes = pd.concat(diabetes_rows, ignore_index=True)
    diabetes_df = all_diabetes.groupby('subject_id')['diabetes_mellitus'].max().reset_index()
    result = result.merge(diabetes_df, on='subject_id', how='left')

# Fill missing diabetes as 0
result['diabetes_mellitus'] = result['diabetes_mellitus'].fillna(0).astype(int)

# ------------------------------
# 8. Save
# ------------------------------
result.to_csv('general_features_complete.csv', index=False)
print("✅ Saved: general_features_complete.csv")

# ------------------------------
# 9. Summary
# ------------------------------
total_patients = len(result)
print(f"\nFinal data: {total_patients} patients")
print(f"Gender distribution: {result['gender'].value_counts().to_dict()}")
print(f"Ethnicity top 5: {result['ethnicity'].value_counts().head(5).to_dict()}")
print(f"Age - Mean: {result['age_years'].mean():.1f}, Range: {result['age_years'].min()}-{result['age_years'].max()}")
print(f"Height available: {result['height_cm'].notna().sum()} patients")
print(f"Weight available: {result['weight_kg'].notna().sum()} patients")
print(f"Diabetes mellitus: {result['diabetes_mellitus'].sum()} patients ({result['diabetes_mellitus'].sum()/total_patients*100:.1f}%)")

print("\nFirst 5 patients:")
print(result.head())

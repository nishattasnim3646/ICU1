# calculate_sofa_complete.py
import pandas as pd
import numpy as np

print("=== CALCULATING SOFA SCORE ===")

# Load our patients
our_patients = pd.read_csv('filtered_patients.csv')['subject_id'].tolist()
print(f"Patients: {len(our_patients)}")

# SOFA component itemids
sofa_components = {
    # Respiration - will calculate PaO2/FiO2 ratio
    'po2': 220224,           # Arterial O2 pressure
    'fio2': 223835,          # Inspired O2 Fraction (FiO2)
    
    # Coagulation
    'platelets': 51265,      # Platelet Count
    
    # Liver  
    'bilirubin': 50884,      # Bilirubin, Indirect
    
    # Cardiovascular
    'map': 220052,           # Mean Arterial Pressure
    'dopamine': 221662,      # Dopamine
    'norepinephrine': 221906, # Norepinephrine
    
    # CNS - Glasgow Coma Scale
    'gcs_eye': 220739,
    'gcs_verbal': 223900, 
    'gcs_motor': 223901,
    
    # Renal
    'creatinine': 50912      # Creatinine
}

# SOFA scoring rules
def sofa_respiration(pao2_fio2):
    if pao2_fio2 >= 400: return 0
    elif pao2_fio2 >= 300: return 1
    elif pao2_fio2 >= 200: return 2
    elif pao2_fio2 >= 100: return 3
    else: return 4

def sofa_coagulation(platelets):
    if platelets >= 150: return 0
    elif platelets >= 100: return 1
    elif platelets >= 50: return 2
    elif platelets >= 20: return 3
    else: return 4

def sofa_liver(bilirubin):
    if bilirubin < 1.2: return 0
    elif bilirubin < 1.9: return 1
    elif bilirubin < 5.9: return 2
    elif bilirubin < 11.9: return 3
    else: return 4

def sofa_cardiovascular(map, vasopressors):
    if vasopressors == 0: return 0
    elif vasopressors == 1: return 2
    elif vasopressors == 2: return 3
    else: return 4

def sofa_cns(gcs):
    if gcs == 15: return 0
    elif gcs >= 13: return 1
    elif gcs >= 10: return 2
    elif gcs >= 6: return 3
    else: return 4

def sofa_renal(creatinine):
    if creatinine < 1.2: return 0
    elif creatinine < 1.9: return 1
    elif creatinine < 3.4: return 2
    elif creatinine < 4.9: return 3
    else: return 4

print("Step 1: Extracting SOFA components...")

# Initialize results
result = pd.DataFrame({'subject_id': our_patients})

# Extract each component (we'll use the lab data we already have)
print("Using previously extracted lab data...")
labs = pd.read_csv('vital_features.csv')

# Add lab components to result
lab_components = ['platelets', 'bilirubin', 'creatinine']
for comp in lab_components:
    result[f'{comp}_min'] = labs[f'{comp}_min']
    result[f'{comp}_max'] = labs[f'{comp}_max']

print("Step 2: Calculating SOFA scores...")

# Calculate SOFA for each patient (using worst values)
sofa_scores = []

for idx, patient in enumerate(our_patients):
    if idx % 5000 == 0:
        print(f"Processing patient {idx}/{len(our_patients)}...")
    
    # Get worst values for this patient
    platelets_worst = result.loc[result['subject_id'] == patient, 'platelets_min'].iloc[0]
    bilirubin_worst = result.loc[result['subject_id'] == patient, 'bilirubin_max'].iloc[0]
    creatinine_worst = result.loc[result['subject_id'] == patient, 'creatinine_max'].iloc[0]
    
    # Calculate SOFA components (simplified - using available data)
    sofa_coag = sofa_coagulation(platelets_worst) if not pd.isna(platelets_worst) else np.nan
    sofa_liv = sofa_liver(bilirubin_worst) if not pd.isna(bilirubin_worst) else np.nan
    sofa_ren = sofa_renal(creatinine_worst) if not pd.isna(creatinine_worst) else np.nan
    
    # For now, use simplified SOFA (3 components)
    sofa_total = np.nansum([sofa_coag, sofa_liv, sofa_ren])
    
    sofa_scores.append({
        'subject_id': patient,
        'sofa_coagulation': sofa_coag,
        'sofa_liver': sofa_liv,
        'sofa_renal': sofa_ren,
        'sofa_total': sofa_total
    })

# Create SOFA results
sofa_df = pd.DataFrame(sofa_scores)
result = result.merge(sofa_df, on='subject_id', how='left')

# Save SOFA scores
result[['subject_id', 'sofa_coagulation', 'sofa_liver', 'sofa_renal', 'sofa_total']].to_csv('sofa_scores.csv', index=False)
print("✅ Saved: sofa_scores.csv")

print(f"\nSOFA Score Summary:")
print(f"Patients with SOFA data: {result['sofa_total'].notna().sum()}")
print(f"SOFA Score range: {result['sofa_total'].min():.1f} to {result['sofa_total'].max():.1f}")
print(f"Mean SOFA: {result['sofa_total'].mean():.2f}")

print("\nFirst 5 patients with SOFA scores:")
print(result[['subject_id', 'sofa_coagulation', 'sofa_liver', 'sofa_renal', 'sofa_total']].head())
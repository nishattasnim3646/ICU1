# extract_diagnosis_features.py
import pandas as pd
import numpy as np
import os

print("=== EXTRACTING DIAGNOSIS FEATURES ===")

# Configuration
data_path = "/home/nishat/physionet.org/files/mimiciv/3.1/"
output_file = "diagnosis.csv"

# Load patient cohort
cohort = pd.read_csv('patients.csv')
our_patients = cohort['subject_id'].unique().tolist()
print(f"Patients: {len(our_patients)}")

# Initialize diagnosis dataframe with subject_id
diagnosis_df = pd.DataFrame({'subject_id': our_patients})

def extract_infection_diagnoses():
    """Extract all infection diagnosis features (binary once)"""
    print("\n=== EXTRACTING INFECTION DIAGNOSES ===")
    
    # Load diagnoses data
    diagnoses = pd.read_csv(os.path.join(data_path, 'hosp/diagnoses_icd.csv'))
    d_icd = pd.read_csv(os.path.join(data_path, 'hosp/d_icd_diagnoses.csv'))
    
    # Filter for our patients
    diagnoses_cohort = diagnoses[diagnoses['subject_id'].isin(our_patients)]
    print(f"Total diagnosis records for our patients: {len(diagnoses_cohort)}")
    
    # ICD-10 codes for infections (comprehensive list)
    infection_codes = {
        'Bile_infection': [
            'K80', 'K81', 'K82', 'K83', 'K85', 'K86', 'K87',  # Gallbladder disorders
        ],
        'Urological_infection': [
            'N10', 'N11', 'N12', 'N13', 'N15', 'N16', 'N30', 'N34', 'N39',  # UTI
        ],
        'Respiratory_infection': [
            'J09', 'J10', 'J11', 'J12', 'J13', 'J14', 'J15', 'J16', 'J18',  # Pneumonia
        ],
        'Skin_infection': [
            'L00', 'L01', 'L02', 'L03', 'L04', 'L05', 'L08',  # Skin infections
        ],
        'Bone_joint_infection': [
            'M00', 'M01', 'M02', 'M86',  # Osteomyelitis and joint infections
        ],
        'Colon_infection': [
            'A04', 'K52', 'A09',  # Gastroenteritis and colitis
        ],
        'Catheter_infection': [
            'T80.2', 'T82.7', 'T83.5', 'T84.5', 'T85.7',  # Device infections
        ],
        'Abdominal_infection': [
            'K35', 'K36', 'K37', 'K38', 'K65',  # Appendicitis and peritonitis
        ],
        'Unknown_infection': [
            'A49', 'B99',  # Unspecified infections
        ]
    }
    
    # Create binary indicators for each infection type
    for infection_type, codes in infection_codes.items():
        # Create pattern for ICD codes
        pattern = '|'.join([f'^{code}' for code in codes])
        
        # Find patients with these infection codes
        infected_patients = diagnoses_cohort[
            diagnoses_cohort['icd_code'].str.match(pattern, na=False)
        ]['subject_id'].unique()
        
        # Create binary column
        diagnosis_df[infection_type] = diagnosis_df['subject_id'].isin(infected_patients).astype(int)
        infected_count = diagnosis_df[infection_type].sum()
        percentage = (infected_count / len(diagnosis_df)) * 100
        print(f"  ✅ {infection_type}: {infected_count} patients ({percentage:.1f}%)")

def extract_diabetes():
    """Extract diabetes diagnosis"""
    print("\n=== EXTRACTING DIABETES DIAGNOSIS ===")
    
    # Load diagnoses data
    diagnoses = pd.read_csv(os.path.join(data_path, 'hosp/diagnoses_icd.csv'))
    
    # ICD-10 codes for diabetes (E10-E14)
    diabetes_codes = ['E10', 'E11', 'E12', 'E13', 'E14']
    
    # Filter for our patients
    diagnoses_cohort = diagnoses[diagnoses['subject_id'].isin(our_patients)]
    
    # Find patients with diabetes
    pattern = '|'.join([f'^{code}' for code in diabetes_codes])
    diabetic_patients = diagnoses_cohort[
        diagnoses_cohort['icd_code'].str.match(pattern, na=False)
    ]['subject_id'].unique()
    
    # Create binary column
    diagnosis_df['Diabetes'] = diagnosis_df['subject_id'].isin(diabetic_patients).astype(int)
    diabetic_count = diagnosis_df['Diabetes'].sum()
    percentage = (diabetic_count / len(diagnosis_df)) * 100
    print(f"  ✅ Diabetes: {diabetic_count} patients ({percentage:.1f}%)")

def load_sofa_scores():
    """Load SOFA scores from existing file"""
    print("\n=== LOADING SOFA SCORES ===")
    
    try:
        sofa_df = pd.read_csv('sofa.csv')
        
        if 'subject_id' in sofa_df.columns:
            # Look for SOFA total columns
            if 'sofa_total_min' in sofa_df.columns and 'sofa_total_max' in sofa_df.columns:
                # Use min and max SOFA scores
                sofa_cols = ['subject_id', 'sofa_total_min', 'sofa_total_max']
                sofa_data = sofa_df[sofa_cols].copy()
                sofa_data = sofa_data.rename(columns={
                    'sofa_total_min': 'SOFA_min',
                    'sofa_total_max': 'SOFA_max'
                })
            elif 'sofa_total' in sofa_df.columns:
                # Use single SOFA total for both min and max
                sofa_data = sofa_df[['subject_id', 'sofa_total']].copy()
                sofa_data['SOFA_min'] = sofa_data['sofa_total']
                sofa_data['SOFA_max'] = sofa_data['sofa_total']
                sofa_data = sofa_data[['subject_id', 'SOFA_min', 'SOFA_max']]
            else:
                # Find any SOFA-related columns
                possible_sofa_cols = [col for col in sofa_df.columns if 'sofa' in col.lower()]
                if possible_sofa_cols:
                    sofa_col = possible_sofa_cols[0]
                    sofa_data = sofa_df[['subject_id', sofa_col]].copy()
                    sofa_data['SOFA_min'] = sofa_data[sofa_col]
                    sofa_data['SOFA_max'] = sofa_data[sofa_col]
                    sofa_data = sofa_data[['subject_id', 'SOFA_min', 'SOFA_max']]
                else:
                    print("  ❌ No SOFA columns found in sofa.csv")
                    return False
            
            # Merge with diagnosis dataframe
            diagnosis_df_updated = diagnosis_df.merge(sofa_data, on='subject_id', how='left')
            
            # Update the global dataframe
            diagnosis_df = diagnosis_df_updated
            
            sofa_count = diagnosis_df['SOFA_min'].notna().sum()
            percentage = (sofa_count / len(diagnosis_df)) * 100
            print(f"  ✅ SOFA scores: {sofa_count} patients ({percentage:.1f}%)")
            return True
            
        else:
            print("  ❌ 'subject_id' column not found in sofa.csv")
            return False
            
    except FileNotFoundError:
        print("  ❌ sofa.csv file not found")
        return False
    except Exception as e:
        print(f"  ❌ Error loading sofa.csv: {e}")
        return False

def add_patient_demographics():
    """Add basic patient demographics for context"""
    print("\n=== ADDING PATIENT DEMOGRAPHICS ===")
    
    try:
        # Load patients data
        patients = pd.read_csv(os.path.join(data_path, 'hosp/patients.csv'))
        admissions = pd.read_csv(os.path.join(data_path, 'hosp/admissions.csv'))
        
        # Filter for our patients
        patients_cohort = patients[patients['subject_id'].isin(our_patients)]
        admissions_cohort = admissions[admissions['subject_id'].isin(our_patients)]
        
        # Add age and gender
        diagnosis_df_with_demo = diagnosis_df.merge(
            patients_cohort[['subject_id', 'gender', 'anchor_age']], 
            on='subject_id', how='left'
        )
        
        # Add ethnicity from admissions
        diagnosis_df_with_demo = diagnosis_df_with_demo.merge(
            admissions_cohort[['subject_id', 'race']].drop_duplicates('subject_id', keep='first'), 
            on='subject_id', how='left'
        )
        
        # Rename for clarity
        diagnosis_df_with_demo = diagnosis_df_with_demo.rename(columns={
            'anchor_age': 'age',
            'race': 'ethnicity'
        })
        
        # Update the global dataframe
        diagnosis_df = diagnosis_df_with_demo
        
        print(f"  ✅ Age: {diagnosis_df['age'].notna().sum()} patients")
        print(f"  ✅ Gender: {diagnosis_df['gender'].notna().sum()} patients")
        print(f"  ✅ Ethnicity: {diagnosis_df['ethnicity'].notna().sum()} patients")
        
    except Exception as e:
        print(f"  ⚠️  Could not add demographics: {e}")

def verify_diagnosis_features():
    """Verify that all diagnosis features are present"""
    print("\n" + "="*60)
    print("FINAL VERIFICATION - DIAGNOSIS FEATURES")
    print("="*60)
    
    # Required diagnosis features
    infection_features = [
        'Bile_infection', 'Urological_infection', 'Respiratory_infection',
        'Skin_infection', 'Bone_joint_infection', 'Colon_infection',
        'Catheter_infection', 'Abdominal_infection', 'Unknown_infection'
    ]
    
    other_features = ['Diabetes', 'SOFA_min', 'SOFA_max']
    
    print("\n📊 INFECTION DIAGNOSES (Binary Once):")
    print("-" * 50)
    print(f"{'Infection Type':<25} {'Patients':<10} {'Percentage':<12}")
    print("-" * 50)
    
    total_infections = 0
    for feature in infection_features:
        if feature in diagnosis_df.columns:
            count = diagnosis_df[feature].sum()
            percentage = (count / len(diagnosis_df)) * 100
            total_infections += count
            print(f"{feature:<25} {count:<10} {percentage:>7.1f}%")
        else:
            print(f"{feature:<25} {'MISSING':<10} {'MISSING':>12}")
    
    print("-" * 50)
    print(f"{'TOTAL INFECTIONS':<25} {total_infections:<10}")
    
    print(f"\n📊 OTHER DIAGNOSIS FEATURES:")
    print("-" * 40)
    for feature in other_features:
        if feature in diagnosis_df.columns:
            if 'SOFA' in feature:
                count = diagnosis_df[feature].notna().sum()
                percentage = (count / len(diagnosis_df)) * 100
                if 'min' in feature:
                    range_str = f"({diagnosis_df[feature].min():.1f}-{diagnosis_df[feature].max():.1f})"
                else:
                    range_str = ""
            else:
                count = diagnosis_df[feature].sum()
                percentage = (count / len(diagnosis_df)) * 100
                range_str = ""
            
            print(f"{feature:<15} {count:<8} patients {percentage:>6.1f}% {range_str}")
        else:
            print(f"{feature:<15} {'MISSING':<8}")
    
    print(f"\nTotal patients in diagnosis.csv: {len(diagnosis_df)}")

def main():
    """Main execution function"""
    # Extract all diagnosis features
    extract_infection_diagnoses()
    extract_diabetes()
    load_sofa_scores()
    add_patient_demographics()  # Optional: add demographics for context
    
    # Final verification
    verify_diagnosis_features()
    
    # Save diagnosis features
    print(f"\n=== SAVING DIAGNOSIS FEATURES ===")
    diagnosis_df.to_csv(output_file, index=False)
    
    # Final summary
    print(f"✅ Saved: {output_file}")
    print(f"📊 Total patients: {len(diagnosis_df)}")
    print(f"🔢 Total columns: {len(diagnosis_df.columns)}")
    
    # Show sample
    print(f"\n📄 Sample of diagnosis data (first 5 patients):")
    sample_cols = ['subject_id', 'Diabetes', 'Bile_infection', 'Respiratory_infection', 'SOFA_min']
    available_cols = [col for col in sample_cols if col in diagnosis_df.columns]
    print(diagnosis_df[available_cols].head())
    
    print(f"\n🎯 DIAGNOSIS FEATURES EXTRACTION COMPLETED!")
    print("File includes:")
    print("  - 9 Infection diagnoses (binary)")
    print("  - Diabetes (binary)") 
    print("  - SOFA scores (min/max)")
    print("  - Patient demographics (age, gender, ethnicity)")
    print("🎉")

if __name__ == "__main__":
    main()
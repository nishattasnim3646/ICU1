# extract_therapy_simple.py
import pandas as pd
import numpy as np
import os

print("=== EXTRACTING THERAPY FEATURES (OPTIMIZED) ===")

# Configuration
data_path = "/home/nishat/physionet.org/files/mimiciv/3.1/"
output_file = "therapy_simple.csv"

# Load patient cohort
print("Loading patient cohort...")
cohort = pd.read_csv('patients.csv')
our_patients = set(cohort['subject_id'].unique())
print(f"Patients: {len(our_patients)}")

# Initialize therapy dataframe with subject_id
therapy_df = pd.DataFrame({'subject_id': list(our_patients)})

def safe_extract_dialysis():
    """Extract dialysis therapy safely with memory management"""
    print("\n=== EXTRACTING DIALYSIS ===")
    
    try:
        # Process in chunks to save memory
        chunks = pd.read_csv(os.path.join(data_path, 'hosp/procedures_icd.csv'), 
                            chunksize=100000)
        
        dialysis_patients = set()
        dialysis_codes = {'5A1D', '5A1D0', '5A1D1', '5A1D2', '5A1D5', '5A1D6', '5A1D7', '5A1D8', '5498'}
        
        for i, chunk in enumerate(chunks):
            # Filter for our patients and dialysis codes
            chunk_filtered = chunk[
                chunk['subject_id'].isin(our_patients) & 
                chunk['icd_code'].isin(dialysis_codes)
            ]
            
            if not chunk_filtered.empty:
                dialysis_patients.update(chunk_filtered['subject_id'].unique())
            
            if i % 10 == 0:
                print(f"  Processed {i+1} chunks...")
        
        therapy_df['Dialysis'] = therapy_df['subject_id'].isin(dialysis_patients).astype(int)
        count = therapy_df['Dialysis'].sum()
        print(f"  ✅ Dialysis: {count} patients ({count/len(therapy_df)*100:.1f}%)")
        
    except Exception as e:
        print(f"  ⚠️  Error: {e}")
        therapy_df['Dialysis'] = 0

def safe_extract_ventilation():
    """Extract mechanical ventilation safely"""
    print("\n=== EXTRACTING MECHANICAL VENTILATION ===")
    
    try:
        # Load d_items first to get ventilation itemids
        d_items = pd.read_csv(os.path.join(data_path, 'icu/d_items.csv'))
        vent_items = set(d_items[
            d_items['label'].str.contains('ventilat|intubat', case=False, na=False)
        ]['itemid'])
        
        # Process procedureevents in chunks
        chunks = pd.read_csv(os.path.join(data_path, 'icu/procedureevents.csv'), 
                            chunksize=100000)
        
        vent_patients = set()
        
        for i, chunk in enumerate(chunks):
            chunk_filtered = chunk[
                chunk['subject_id'].isin(our_patients) & 
                chunk['itemid'].isin(vent_items)
            ]
            
            if not chunk_filtered.empty:
                vent_patients.update(chunk_filtered['subject_id'].unique())
            
            if i % 10 == 0:
                print(f"  Processed {i+1} chunks...")
        
        therapy_df['Mechanical_Ventilation'] = therapy_df['subject_id'].isin(vent_patients).astype(int)
        count = therapy_df['Mechanical_Ventilation'].sum()
        print(f"  ✅ Mechanical Ventilation: {count} patients ({count/len(therapy_df)*100:.1f}%)")
        
    except Exception as e:
        print(f"  ⚠️  Error: {e}")
        therapy_df['Mechanical_Ventilation'] = 0

def safe_extract_vasopressors():
    """Extract vasopressors safely with chunk processing"""
    print("\n=== EXTRACTING VASOPRESSORS ===")
    
    vasopressor_config = {
        'Epinephrine': {221289, 30047, 30120},
        'Norepinephrine': {221906, 30051, 30128},
        'Dopamine': {221662, 30043, 30119},
        'Dobutamine': {221653, 30042, 30125}
    }
    
    try:
        # Process inputevents in chunks
        chunks = pd.read_csv(os.path.join(data_path, 'icu/inputevents.csv'), 
                            chunksize=100000)
        
        vasopressor_patients = {name: set() for name in vasopressor_config.keys()}
        
        for i, chunk in enumerate(chunks):
            chunk_filtered = chunk[chunk['subject_id'].isin(our_patients)]
            
            for vasopressor, itemids in vasopressor_config.items():
                patients_with_vaso = chunk_filtered[
                    chunk_filtered['itemid'].isin(itemids)
                ]['subject_id'].unique()
                
                if len(patients_with_vaso) > 0:
                    vasopressor_patients[vasopressor].update(patients_with_vaso)
            
            if i % 10 == 0:
                print(f"  Processed {i+1} chunks...")
        
        # Add binary columns
        for vasopressor, patients_set in vasopressor_patients.items():
            therapy_df[vasopressor] = therapy_df['subject_id'].isin(patients_set).astype(int)
            count = therapy_df[vasopressor].sum()
            print(f"  ✅ {vasopressor}: {count} patients ({count/len(therapy_df)*100:.1f}%)")
        
    except Exception as e:
        print(f"  ⚠️  Error: {e}")
        for vasopressor in vasopressor_config.keys():
            therapy_df[vasopressor] = 0

def safe_extract_doses():
    """Extract vasopressor doses with minimal memory usage"""
    print("\n=== EXTRACTING VASOPRESSOR DOSES ===")
    
    dose_config = {
        'Epinephrine_dose': {221289, 30047, 30120},
        'Norepinephrine_dose': {221906, 30051, 30128},
        'Dopamine_dose': {221662, 30043, 30119},
        'Dobutamine_dose': {221653, 30042, 30125}
    }
    
    try:
        # Initialize dose columns with NaN
        for dose_col in dose_config.keys():
            therapy_df[dose_col] = np.nan
        
        # Process in chunks and update doses
        chunks = pd.read_csv(os.path.join(data_path, 'icu/inputevents.csv'), 
                            chunksize=50000,
                            usecols=['subject_id', 'itemid', 'rate'])
        
        for i, chunk in enumerate(chunks):
            chunk_filtered = chunk[
                chunk['subject_id'].isin(our_patients) & 
                chunk['rate'].notna() & 
                (chunk['rate'] > 0)
            ]
            
            for dose_col, itemids in dose_config.items():
                dose_data = chunk_filtered[chunk_filtered['itemid'].isin(itemids)]
                
                if not dose_data.empty:
                    # Update maximum doses
                    current_max = dose_data.groupby('subject_id')['rate'].max()
                    
                    for subject_id, max_rate in current_max.items():
                        current_val = therapy_df.loc[therapy_df['subject_id'] == subject_id, dose_col].values
                        if len(current_val) > 0 and (np.isnan(current_val[0]) or max_rate > current_val[0]):
                            therapy_df.loc[therapy_df['subject_id'] == subject_id, dose_col] = max_rate
            
            if i % 20 == 0:
                print(f"  Processed {i+1} chunks...")
        
        # Print dose statistics
        for dose_col in dose_config.keys():
            count = therapy_df[dose_col].notna().sum()
            if count > 0:
                avg_dose = therapy_df[dose_col].mean()
                print(f"  ✅ {dose_col}: {count} patients, avg: {avg_dose:.3f} mcg/kg/min")
            else:
                print(f"  ⚠️  {dose_col}: No dose data")
                
    except Exception as e:
        print(f"  ⚠️  Error extracting doses: {e}")

def add_demographics_safe():
    """Add demographics safely"""
    print("\n=== ADDING DEMOGRAPHICS ===")
    
    try:
        patients = pd.read_csv(os.path.join(data_path, 'hosp/patients.csv'))
        patients_cohort = patients[patients['subject_id'].isin(our_patients)]
        
        therapy_df['age'] = therapy_df['subject_id'].map(
            patients_cohort.set_index('subject_id')['anchor_age']
        )
        therapy_df['gender'] = therapy_df['subject_id'].map(
            patients_cohort.set_index('subject_id')['gender']
        )
        
        print(f"  ✅ Age: {therapy_df['age'].notna().sum()} patients")
        print(f"  ✅ Gender: {therapy_df['gender'].notna().sum()} patients")
        
    except Exception as e:
        print(f"  ⚠️  Error: {e}")

def save_intermediate():
    """Save intermediate results to avoid data loss"""
    print(f"\n💾 Saving intermediate results...")
    therapy_df.to_csv('therapy_intermediate.csv', index=False)
    print("✅ Saved therapy_intermediate.csv")

def main():
    """Main function with progress saving"""
    try:
        # Extract features one by one with progress saving
        safe_extract_dialysis()
        save_intermediate()
        
        safe_extract_ventilation()
        save_intermediate()
        
        safe_extract_vasopressors()
        save_intermediate()
        
        safe_extract_doses()
        save_intermediate()
        
        add_demographics_safe()
        
        # Final save
        print(f"\n=== SAVING FINAL RESULTS ===")
        therapy_df.to_csv(output_file, index=False)
        
        # Summary
        print(f"✅ Saved: {output_file}")
        print(f"📊 Total patients: {len(therapy_df)}")
        print(f"🔢 Total columns: {len(therapy_df.columns)}")
        
        print(f"\n🎯 THERAPY EXTRACTION COMPLETED!")
        print("Features extracted:")
        for col in therapy_df.columns:
            if col != 'subject_id':
                if 'dose' in col:
                    count = therapy_df[col].notna().sum()
                    print(f"  - {col}: {count} patients")
                else:
                    count = therapy_df[col].sum() if therapy_df[col].dtype == 'int64' else therapy_df[col].notna().sum()
                    print(f"  - {col}: {count} patients")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print("💾 Attempting to save current progress...")
        therapy_df.to_csv('therapy_emergency_save.csv', index=False)
        print("✅ Progress saved to therapy_emergency_save.csv")

if __name__ == "__main__":
    main()
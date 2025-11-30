# extract_final_essential_features.py
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

print("=== EXTRACTING FINAL ESSENTIAL FEATURES ===")

# Configuration
data_path = "/home/nishat/physionet.org/files/mimiciv/3.1/"
output_file = "FINAL_ESSENTIAL_FEATURES.csv"

# Load patient cohort
cohort = pd.read_csv('patients.csv')
our_patients = cohort['subject_id'].unique().tolist()
print(f"Patients: {len(our_patients)}")

# Get ICU admission times for time window filtering
cohort_times = cohort[['subject_id', 'intime']].copy()
cohort_times['intime'] = pd.to_datetime(cohort_times['intime'])
cohort_times['end_time'] = cohort_times['intime'] + timedelta(hours=30)

# ESSENTIAL FEATURES MAPPING - ONLY the columns you specified
ESSENTIAL_FEATURES = {
    # Blood Gas & Oxygenation
    'PO2': [220224, 490, 50821, 50816],  # mmHg
    'FiO2': [223835, 3420, 3422, 189, 190],  # %
    'SpO2': [220277, 646, 834],  # %
    
    # Laboratory Values
    'Bilirubin': [50885, 50884, 4948, 4949],  # mg/dl
    'Lactate': [50813, 818, 1531],  # mmol/l
    'CRP': [50889],  # mg/l
    'Leukocytes': [51301, 51300, 51302, 51303],  # /nl
    'Blood sugar': [50809, 50931, 807, 811, 1529],  # mg/dl (Blood sugar)
    'Platelets': [51265, 51256, 52769],  # 10³/mm³
    'Creatinine': [50912, 791, 1525],  # mg/dl
    
    # Blood Pressure
    'Systolic_BP': [220050, 51, 455, 6701, 442, 6701],  # mmHg
    'Diastolic_BP': [220051, 8368, 8441, 8555, 443, 8440],  # mmHg
    'MAP': [220052, 456, 52, 6702, 444],  # mmHg (Mean Blood Pressure)
    
    # Vital Signs
    'Respiratory_Rate': [220210, 618, 615, 614, 651],  # /min
    'Heart_Rate': [220045, 211, 220046],  # /min
    'Temperature': [223762, 676, 677, 678, 223761, 679],  # °C
    
    # Output
    'Urine_Output': [226559, 226560, 227510, 227489],  # ml/h
    
    # Neurological Scores
    'GCS_Total': [198, 226755, 227013],  # 3-15 scale
    'GCS_Eye': [220739, 184],
    'GCS_Verbal': [223900, 723],
    'GCS_Motor': [223901, 454],
}

# REQUIRED COLUMNS - EXACTLY as you specified
REQUIRED_COLUMNS = [
    'PO2', 'FiO2', 'Bilirubin', 'Lactate', 'Systolic_BP', 'Diastolic_BP', 
    'MAP', 'CRP', 'Leukocytes', 'Urine_Output', 'Blood sugar', 'Respiratory_Rate', 
    'Heart_Rate', 'Platelets', 'Creatinine', 'Temperature', 'SOFA_score', 'GCS_Total'
]

# Initialize results dataframe
results = pd.DataFrame({'subject_id': our_patients})

# Add all required columns (initialize with NaN)
for feature in REQUIRED_COLUMNS:
    if feature != 'SOFA_score':  # SOFA will be loaded separately
        results[f'{feature}_min'] = np.nan
        results[f'{feature}_max'] = np.nan

def extract_feature(feature_name, itemids, source_file, value_column='valuenum'):
    """Extract a single feature with comprehensive error handling"""
    print(f"  Extracting {feature_name}...")
    
    all_data = []
    
    try:
        file_path = os.path.join(data_path, source_file)
        chunks = pd.read_csv(file_path, chunksize=500000,
                            usecols=['subject_id', 'itemid', 'charttime', value_column])
        
        for chunk_idx, chunk in enumerate(chunks):
            # Filter for our patients and valid values
            chunk = chunk[chunk['subject_id'].isin(our_patients)]
            chunk = chunk[chunk[value_column].notna() & (chunk[value_column] > 0)]
            
            if chunk.empty:
                continue
                
            # Merge with time windows to get first 30 hours only
            chunk_with_time = chunk.merge(cohort_times, on='subject_id', how='inner')
            chunk_with_time['charttime'] = pd.to_datetime(chunk_with_time['charttime'])
            
            chunk_filtered = chunk_with_time[
                (chunk_with_time['charttime'] >= chunk_with_time['intime']) &
                (chunk_with_time['charttime'] <= chunk_with_time['end_time'])
            ]
            
            # Extract feature data
            feature_data = chunk_filtered[chunk_filtered['itemid'].isin(itemids)]
            
            if not feature_data.empty:
                all_data.append(feature_data[['subject_id', value_column]])
            
            if chunk_idx % 20 == 0 and chunk_idx > 0:
                print(f"    Processed {chunk_idx + 1} chunks...")
        
        # Aggregate results
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            stats = combined_data.groupby('subject_id')[value_column].agg(['min', 'max']).reset_index()
            
            # Update results
            for _, row in stats.iterrows():
                subject_id = row['subject_id']
                mask = results['subject_id'] == subject_id
                results.loc[mask, f'{feature_name}_min'] = row['min']
                results.loc[mask, f'{feature_name}_max'] = row['max']
            
            print(f"    ✅ {feature_name}: {len(stats)} patients")
            return True
        else:
            print(f"    ❌ {feature_name}: No data found")
            return False
            
    except Exception as e:
        print(f"    ⚠️  Error extracting {feature_name}: {e}")
        return False

def extract_lab_features():
    """Extract laboratory features"""
    print("\n=== EXTRACTING LABORATORY FEATURES ===")
    
    lab_features = [
        'Bilirubin', 'Lactate', 'CRP', 'Leukocytes', 'Blood sugar', 
        'Platelets', 'Creatinine'
    ]
    
    for feature in lab_features:
        extract_feature(feature, ESSENTIAL_FEATURES[feature], 'hosp/labevents.csv')

def extract_chart_features():
    """Extract chart features"""
    print("\n=== EXTRACTING CHART FEATURES ===")
    
    chart_features = [
        'PO2', 'FiO2', 'SpO2', 'Systolic_BP', 'Diastolic_BP', 'MAP',
        'Respiratory_Rate', 'Heart_Rate', 'Temperature',
        'GCS_Total', 'GCS_Eye', 'GCS_Verbal', 'GCS_Motor'
    ]
    
    for feature in chart_features:
        extract_feature(feature, ESSENTIAL_FEATURES[feature], 'icu/chartevents.csv')

def extract_urine_output():
    """Extract urine output"""
    print("\n=== EXTRACTING URINE OUTPUT ===")
    extract_feature('Urine_Output', ESSENTIAL_FEATURES['Urine_Output'], 
                   'icu/outputevents.csv', 'value')

def ensure_gcs_completeness():
    """Ensure GCS total is complete"""
    print("\n=== ENSURING GCS TOTAL COMPLETENESS ===")
    
    missing_gcs = results['GCS_Total_min'].isna().sum()
    print(f"  Patients missing direct GCS: {missing_gcs}")
    
    if missing_gcs > 0 and all(col in results.columns for col in ['GCS_Eye_min', 'GCS_Verbal_min', 'GCS_Motor_min']):
        
        mask_missing = results['GCS_Total_min'].isna()
        
        if mask_missing.any():
            # Calculate from components
            gcs_min_calc = (
                results.loc[mask_missing, 'GCS_Eye_min'].fillna(0) + 
                results.loc[mask_missing, 'GCS_Verbal_min'].fillna(0) + 
                results.loc[mask_missing, 'GCS_Motor_min'].fillna(0)
            )
            
            gcs_max_calc = (
                results.loc[mask_missing, 'GCS_Eye_max'].fillna(0) + 
                results.loc[mask_missing, 'GCS_Verbal_max'].fillna(0) + 
                results.loc[mask_missing, 'GCS_Motor_max'].fillna(0)
            )
            
            # Only use if all components are present
            valid_min = (
                results.loc[mask_missing, 'GCS_Eye_min'].notna() & 
                results.loc[mask_missing, 'GCS_Verbal_min'].notna() & 
                results.loc[mask_missing, 'GCS_Motor_min'].notna()
            )
            
            valid_max = (
                results.loc[mask_missing, 'GCS_Eye_max'].notna() & 
                results.loc[mask_missing, 'GCS_Verbal_max'].notna() & 
                results.loc[mask_missing, 'GCS_Motor_max'].notna()
            )
            
            results.loc[mask_missing & valid_min, 'GCS_Total_min'] = gcs_min_calc[valid_min]
            results.loc[mask_missing & valid_max, 'GCS_Total_max'] = gcs_max_calc[valid_max]
            
            calculated_count = valid_min.sum()
            print(f"  ✅ Added GCS from components for {calculated_count} patients")

def load_sofa_scores():
    """Load SOFA scores from existing file"""
    print("\n=== LOADING SOFA SCORES ===")
    
    try:
        sofa_df = pd.read_csv('sofa.csv')
        
        if 'subject_id' in sofa_df.columns:
            # Look for SOFA columns
            sofa_cols = ['subject_id']
            
            if 'sofa_total_min' in sofa_df.columns and 'sofa_total_max' in sofa_df.columns:
                sofa_cols.extend(['sofa_total_min', 'sofa_total_max'])
            elif 'sofa_total' in sofa_df.columns:
                sofa_df['SOFA_score_min'] = sofa_df['sofa_total']
                sofa_df['SOFA_score_max'] = sofa_df['sofa_total']
                sofa_cols.extend(['SOFA_score_min', 'SOFA_score_max'])
            else:
                # Find any SOFA-related columns
                possible_sofa_cols = [col for col in sofa_df.columns if 'sofa' in col.lower()]
                if possible_sofa_cols:
                    # Use the first SOFA column found for both min and max
                    sofa_col = possible_sofa_cols[0]
                    sofa_df['SOFA_score_min'] = sofa_df[sofa_col]
                    sofa_df['SOFA_score_max'] = sofa_df[sofa_col]
                    sofa_cols.extend(['SOFA_score_min', 'SOFA_score_max'])
                else:
                    print("  ❌ No SOFA columns found")
                    return False
            
            # Merge with results
            global results
            results = results.merge(sofa_df[sofa_cols], on='subject_id', how='left')
            
            sofa_count = results[sofa_cols[1]].notna().sum()
            print(f"  ✅ SOFA scores loaded for {sofa_count} patients")
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

def verify_all_columns():
    """Verify that ALL required columns are present"""
    print("\n" + "="*60)
    print("FINAL VERIFICATION - ALL REQUIRED COLUMNS")
    print("="*60)
    
    all_columns_present = True
    missing_columns = []
    
    print("\n📊 REQUIRED COLUMNS STATUS:")
    print("-" * 50)
    
    for feature in REQUIRED_COLUMNS:
        min_col = f'{feature}_min'
        max_col = f'{feature}_max'
        
        if min_col in results.columns and max_col in results.columns:
            min_count = results[min_col].notna().sum()
            max_count = results[max_col].notna().sum()
            status = "✅ PRESENT"
            print(f"{feature:<20} {min_count:<8} patients {status}")
        else:
            print(f"{feature:<20} {'MISSING':<8}          ❌ MISSING")
            missing_columns.append(feature)
            all_columns_present = False
    
    print("-" * 50)
    print(f"Total patients: {len(results)}")
    
    if all_columns_present:
        print(f"\n🎉 SUCCESS: All {len(REQUIRED_COLUMNS)} required columns are PRESENT!")
    else:
        print(f"\n⚠️  WARNING: Missing columns: {missing_columns}")
    
    return all_columns_present

def main():
    """Main execution function"""
    # Extract all essential features
    extract_lab_features()
    extract_chart_features()
    extract_urine_output()
    ensure_gcs_completeness()
    load_sofa_scores()
    
    # Final verification
    verify_all_columns()
    
    # Select only the required columns for final output
    final_columns = ['subject_id']
    for feature in REQUIRED_COLUMNS:
        final_columns.extend([f'{feature}_min', f'{feature}_max'])
    
    # Keep only columns that exist
    available_columns = [col for col in final_columns if col in results.columns]
    final_results = results[available_columns]
    
    # Save final results
    print(f"\n=== SAVING FINAL RESULTS ===")
    final_results.to_csv(output_file, index=False)
    
    # Final summary
    print(f"✅ Saved: {output_file}")
    print(f"📊 Total patients: {len(final_results)}")
    print(f"🔢 Total columns: {len(final_results.columns)}")
    
    # Show column names
    print(f"\n📋 FINAL COLUMNS:")
    for col in final_results.columns:
        if col != 'subject_id':
            available = final_results[col].notna().sum()
            print(f"  {col}: {available} patients")
    
    print(f"\n🎯 FINAL ESSENTIAL FEATURES EXTRACTION COMPLETED!")
    print("All your specified columns are INCLUDED! 🎉")

if __name__ == "__main__":
    main()
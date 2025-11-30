# extract_antibiotics.py
import pandas as pd

print("=== EXTRACTING ANTIBIOTIC DATA ===")

# Load all files that might contain antibiotics
print("Loading files...")

antibiotics_data = []

# 1. Check prescriptions.csv
try:
    prescriptions = pd.read_csv('hosp/prescriptions.csv')
    rx_abx = prescriptions[prescriptions['drug'].str.contains(
        'vancomycin|cefepime|piperacillin|tazobactam|meropenem|cefazolin', 
        case=False, na=False
    )]
    if len(rx_abx) > 0:
        rx_abx['source_file'] = 'prescriptions.csv'
        rx_abx['antibiotic'] = rx_abx['drug']
        antibiotics_data.append(rx_abx[['subject_id', 'hadm_id', 'antibiotic', 'source_file']])
        print(f"Found {len(rx_abx)} in prescriptions.csv")
except Exception as e:
    print(f"Error reading prescriptions: {e}")

# 2. Check pharmacy.csv
try:
    pharmacy = pd.read_csv('hosp/pharmacy.csv')
    pharm_abx = pharmacy[pharmacy['medication'].str.contains(
        'vancomycin|cefepime|piperacillin|tazobactam|meropenem|cefazolin',
        case=False, na=False
    )]
    if len(pharm_abx) > 0:
        pharm_abx['source_file'] = 'pharmacy.csv'
        pharm_abx['antibiotic'] = pharm_abx['medication']
        antibiotics_data.append(pharm_abx[['subject_id', 'hadm_id', 'antibiotic', 'source_file']])
        print(f"Found {len(pharm_abx)} in pharmacy.csv")
except Exception as e:
    print(f"Error reading pharmacy: {e}")

# 3. Check emar.csv
try:
    emar = pd.read_csv('hosp/emar.csv')
    emar_abx = emar[emar['medication'].str.contains(
        'vancomycin|cefepime|piperacillin|tazobactam|meropenem|cefazolin',
        case=False, na=False
    )]
    if len(emar_abx) > 0:
        emar_abx['source_file'] = 'emar.csv'
        emar_abx['antibiotic'] = emar_abx['medication']
        antibiotics_data.append(emar_abx[['subject_id', 'hadm_id', 'antibiotic', 'source_file']])
        print(f"Found {len(emar_abx)} in emar.csv")
except Exception as e:
    print(f"Error reading emar: {e}")

# 4. Check microbiologyevents.csv
try:
    micro = pd.read_csv('hosp/microbiologyevents.csv')
    micro_abx = micro[micro['ab_name'].str.contains(
        'vancomycin|cefepime|piperacillin|tazobactam|meropenem|cefazolin',
        case=False, na=False
    )]
    if len(micro_abx) > 0:
        micro_abx['source_file'] = 'microbiologyevents.csv'
        micro_abx['antibiotic'] = micro_abx['ab_name']
        antibiotics_data.append(micro_abx[['subject_id', 'hadm_id', 'antibiotic', 'source_file']])
        print(f"Found {len(micro_abx)} in microbiologyevents.csv")
except Exception as e:
    print(f"Error reading micro: {e}")

# Combine all antibiotic data
if antibiotics_data:
    all_antibiotics = pd.concat(antibiotics_data, ignore_index=True)
    
    # Group by hadm_id and combine sources
    abx_summary = all_antibiotics.groupby(['subject_id', 'hadm_id', 'antibiotic'])['source_file'].agg([
        ('source_files', lambda x: ', '.join(sorted(set(x)))),
        ('count', 'count')
    ]).reset_index()
    
    # Save
    abx_summary.to_csv('antibiotics_merged.csv', index=False)
    print(f"\n✅ Saved: antibiotics_merged.csv")
    print(f"Total antibiotic records: {len(all_antibiotics)}")
    print(f"Unique hadm_id with antibiotics: {abx_summary['hadm_id'].nunique()}")
    
    print("\nFirst 10 records:")
    print(abx_summary.head(10))
else:
    print("No antibiotic data found!")
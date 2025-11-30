# extract_icu_antibiotics.py
import pandas as pd

print("=== EXTRACTING ANTIBIOTICS FROM ICU FILES ===")

# Antibiotic itemids from d_items.csv
abx_itemids = {
    'Vancomycin': 225798,
    'Cefepime': 225851, 
    'Piperacillin/Tazobactam': 225893,
    'Meropenem': 225883,
    'Cefazolin': 225850
}

antibiotics_data = []

# Search inputevents.csv (IV medications)
print("Searching inputevents.csv for antibiotics...")
try:
    chunks = pd.read_csv('icu/inputevents.csv', chunksize=50000, 
                        usecols=['subject_id', 'hadm_id', 'stay_id', 'itemid', 'amount', 'rate'])
    
    for i, chunk in enumerate(chunks):
        # Filter for antibiotic itemids
        abx_chunk = chunk[chunk['itemid'].isin(abx_itemids.values())]
        if len(abx_chunk) > 0:
            # Map itemid to antibiotic name
            itemid_to_name = {v: k for k, v in abx_itemids.items()}
            abx_chunk['antibiotic'] = abx_chunk['itemid'].map(itemid_to_name)
            abx_chunk['source_file'] = 'inputevents.csv'
            antibiotics_data.append(abx_chunk[['subject_id', 'hadm_id', 'stay_id', 'antibiotic', 'source_file']])
        
        if i % 10 == 0:
            print(f"  Processed {i+1} chunks...")
            
except Exception as e:
    print(f"Error: {e}")

if antibiotics_data:
    all_abx = pd.concat(antibiotics_data, ignore_index=True)
    
    # Save ICU antibiotics
    all_abx.to_csv('icu_antibiotics.csv', index=False)
    print(f"✅ Saved: icu_antibiotics.csv")
    print(f"Found {len(all_abx)} antibiotic administrations in ICU")
    print(f"Unique patients: {all_abx['subject_id'].nunique()}")
    
    print("\nAntibiotics found in ICU:")
    print(all_abx['antibiotic'].value_counts())
    
    print("\nFirst 5 records:")
    print(all_abx.head())
else:
    print("No antibiotics found in ICU files")
import pandas as pd

d_items = pd.read_csv('icu/d_items.csv')
antibiotics = ['vancomycin', 'cefepime', 'piperacillin', 'meropenem', 'cefazolin']

abx_items = d_items[d_items['label'].str.contains('|'.join(antibiotics), case=False, na=False)]
print("Antibiotic itemids in d_items.csv:")
print(abx_items[['itemid', 'label']])
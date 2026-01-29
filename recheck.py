# Run this command directly in your terminal
python3 -c "
import pandas as pd
df = pd.read_csv('antibiotics_multipleRow.csv')
df.to_excel('antibiotics_multipleRow.xlsx', index=False)
print('✅ Converted antibiotics_multipleRow.csv to antibiotics_multipleRow.xlsx')
"
import pandas as pd

# Load files
gen  = pd.read_csv("general.csv")
vital = pd.read_csv("vital.csv")
diag = pd.read_csv("diagnosis.csv")
ther = pd.read_csv("therapy.csv")

print("Shapes:")
print("general:", gen.shape)
print("vital:", vital.shape)
print("diagnosis:", diag.shape)
print("therapy:", ther.shape)

print("\nMerging based on subject_id ONLY...")

# Step 1: merge general + vital
m1 = gen.merge(vital, on="subject_id", how="left")

# Step 2: merge diagnosis
m2 = m1.merge(diag, on="subject_id", how="left")

# Step 3: merge therapy
final = m2.merge(ther, on="subject_id", how="left")

# Save
final.to_csv("merged_on_subject_id.csv", index=False)

print("\nSaved: merged_on_subject_id.csv")
print("Final shape:", final.shape)

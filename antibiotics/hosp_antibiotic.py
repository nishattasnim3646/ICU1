import pandas as pd

# File paths
presc_path = "/home/nishat/physionet.org/files/mimiciv/3.1/hosp/prescriptions.csv"
pharm_path = "/home/nishat/physionet.org/files/mimiciv/3.1/hosp/pharmacy.csv"
emar_path  = "/home/nishat/physionet.org/files/mimiciv/3.1/hosp/emar.csv"
micro_path = "/home/nishat/physionet.org/files/mimiciv/3.1/hosp/microbiologyevents.csv"

# Target antibiotics
targets = [
    "vancomycin",
    "cefepime",
    "piperacillin-tazobactam",
    "piperacillin–tazobactam",
    "meropenem",
    "cefazolin"
]

# Normalize function
def norm(text):
    if pd.isna(text): return ""
    return text.replace("–", "-").lower().strip()

# Store all results here
records = []

def process_file(path, cols, med_col, source_name):
    print(f"Processing {source_name}...")
    for chunk in pd.read_csv(path, usecols=cols, chunksize=100000):

        # Normalize antibiotic column
        chunk["ab_norm"] = chunk[med_col].apply(norm)

        # Filter rows
        mask = chunk["ab_norm"].apply(lambda x: any(t in x for t in targets))
        matches = chunk.loc[mask].copy()

        # Extract matched antibiotic name
        def get_ab(x):
            x = norm(x)
            for t in targets:
                if t.lower() in x:
                    return t.replace("-", "–")  # prettier
            return None

        matches["antibiotic"] = matches[med_col].apply(get_ab)
        matches["source"] = source_name

        # Append needed columns only
        for _, row in matches.iterrows():
            records.append({
                "subject_id": row.get("subject_id", None),
                "hadm_id": row.get("hadm_id", None),
                "antibiotic": row["antibiotic"],
                "source": source_name
            })


# ---- Process all 4 files ----
process_file(presc_path, ["subject_id", "hadm_id", "drug"], "drug", "prescriptions")
process_file(pharm_path, ["subject_id", "hadm_id", "medication"], "medication", "pharmacy")
process_file(emar_path,  ["subject_id", "hadm_id", "medication"], "medication", "emar")
process_file(micro_path, ["subject_id", "hadm_id", "ab_name"], "ab_name", "microbiology")

# Convert to DataFrame
df = pd.DataFrame(records)

# If multiple sources for same patient & antibiotic → group them
df_final = (
    df.groupby(["subject_id", "hadm_id", "antibiotic"])
      .agg({"source": lambda x: "; ".join(sorted(set(x)))})
      .reset_index()
)

# Save final merged file
output = "merged_antibiotic_records.csv"
df_final.to_csv(output, index=False)

print("\nSaved final merged file:", output)
print("Total matched rows:", len(df_final))

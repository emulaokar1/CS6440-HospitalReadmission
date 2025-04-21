
import pandas as pd
from datetime import datetime, timedelta

# Load OMOP tables (SYNTHEA data)
patient = pd.read_csv("data/patients.csv")
visit = pd.read_csv("data/encounters.csv")
condition = pd.read_csv("data/conditions.csv")
drug = pd.read_csv("data/medications.csv")
measurement = pd.read_csv("data/observations.csv")
procedure = pd.read_csv("data/procedures.csv")

# Prepare visit table
visit.rename(columns={"Id": "VISIT_ID"}, inplace=True)
visit["visit_start_date"] = pd.to_datetime(visit["START"], errors="coerce").dt.tz_localize(None)
visit["visit_end_date"] = pd.to_datetime(visit["STOP"], errors="coerce").dt.tz_localize(None)
visit["length_of_stay"] = (visit["visit_end_date"] - visit["visit_start_date"]).dt.total_seconds() / 3600

# Merge with person table
merged = visit.merge(patient, left_on="PATIENT", right_on="Id", how="left")
merged["BIRTHDATE"] = pd.to_datetime(merged["BIRTHDATE"], errors="coerce").dt.tz_localize(None)
merged["age_at_visit"] = (merged["visit_start_date"] - merged["BIRTHDATE"]).dt.days // 365

# One-hot encode GENDER, RACE, and ENCOUNTERCLASS
merged = pd.get_dummies(merged, columns=["GENDER", "RACE", "ENCOUNTERCLASS"],
                        prefix=["GENDER", "RACE", "CLASS"])

# Aggregate condition count
condition_counts = condition.groupby("ENCOUNTER").size().reset_index(name="num_conditions")
merged = merged.merge(
    condition_counts, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna({"num_conditions": 0})

# Aggregate drug count
drug_counts = drug.groupby("ENCOUNTER")["CODE"].nunique().reset_index(name="num_unique_drugs")
merged = merged.merge(
    drug_counts, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna({"num_unique_drugs": 0})

# Aggregate procedure count
procedure_counts = procedure.groupby("ENCOUNTER").size().reset_index(name="num_procedures")
merged = merged.merge(
    procedure_counts, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna({"num_procedures": 0})

# Aggregate measurement count
measurement_counts = measurement.groupby("ENCOUNTER").size().reset_index(name="num_measurements")
merged = merged.merge(
    measurement_counts, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna({"num_measurements": 0})

# Compute 30-day readmission label
merged = merged.sort_values(["PATIENT", "visit_start_date"])
merged["next_visit_start"] = merged.groupby("PATIENT")["visit_start_date"].shift(-1)
merged["next_visit_gap_days"] = (merged["next_visit_start"] - merged["visit_end_date"]).dt.days
merged["readmitted_30_days"] = (merged["next_visit_gap_days"] <= 30).astype(int)
merged["readmitted_60_days"] = (merged["next_visit_gap_days"] <= 60).astype(int)

# Step 1: Clean and lowercase descriptions
condition["DESCRIPTION"] = condition["DESCRIPTION"].astype(str).str.lower()

# Step 2: Define 8 key condition flags using keywords
flags = {
    "has_heart_disease": "heart|coronary|myocardial|cvd|angina|mi",
    "has_diabetes": "diabetes",
    "has_copd": "copd|bronchitis|emphysema",
    "has_cancer": "cancer|carcinoma|tumor|malignancy|neoplasm",
    "has_hypertension": "hypertension|htn|high blood pressure",
    "has_kidney_disease": "kidney|renal failure|ckd",
    "has_stroke": "stroke|cva|cerebrovascular",
    "has_depression": "depression|depressive|mood disorder",
}

for flag, pattern in flags.items():
    condition[flag] = condition["DESCRIPTION"].str.contains(pattern, na=False)

# Step 3 (optional but recommended): filter out vague entries
# Only keep conditions that match at least one of the flags
flag_columns = list(flags.keys())
condition["has_any_flag"] = condition[flag_columns].any(axis=1)
condition_filtered = condition[condition["has_any_flag"]].copy()

# Step 4: Aggregate per ENCOUNTER (i.e., per VISIT_ID)
condition_flags_agg = condition_filtered.groupby("ENCOUNTER")[flag_columns].max().reset_index()

# Step 5: Merge into main `merged` dataset
merged = merged.merge(
    condition_flags_agg, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna(0)


disease_flags = [
    "has_heart_disease", "has_diabetes", "has_copd", "has_cancer",
    "has_hypertension", "has_kidney_disease", "has_stroke", "has_depression"
]

# Clean up the type after merging
for flag in disease_flags:
    merged[flag] = merged[flag].astype(int)

# Step 1: Clean medication descriptions
drug["DESCRIPTION"] = drug["DESCRIPTION"].astype(str).str.lower()

# Step 2: Define keyword-based medication flags
drug["is_opioid"] = drug["DESCRIPTION"].str.contains("morphine|fentanyl|oxycodone|hydrocodone", na=False)
drug["is_insulin"] = drug["DESCRIPTION"].str.contains("insulin", na=False)
drug["is_anticoagulant"] = drug["DESCRIPTION"].str.contains("warfarin|heparin|apixaban|xarelto", na=False)
drug["is_antipsychotic"] = drug["DESCRIPTION"].str.contains("haloperidol|risperidone|olanzapine", na=False)
drug["is_antibiotic"] = drug["DESCRIPTION"].str.contains("penicillin|amoxicillin|azithromycin", na=False)
drug["is_statins"] = drug["DESCRIPTION"].str.contains("atorvastatin|simvastatin", na=False)

# Step 3: Aggregate max per encounter
medication_flags = [
    "is_opioid", "is_insulin", "is_anticoagulant", "is_antipsychotic", "is_antibiotic", "is_statins"
]
drug_flags_agg = drug.groupby("ENCOUNTER")[medication_flags].max().reset_index()

# Step 4: Merge into merged DataFrame
merged = merged.merge(
    drug_flags_agg, left_on="VISIT_ID", right_on="ENCOUNTER", how="left"
).drop(columns=["ENCOUNTER"]).fillna(0)

# Step 5: Cast to int
for flag in medication_flags:
    merged[flag] = merged[flag].astype(int)

merged["has_chronic_condition"] = (
    merged["has_heart_disease"] |
    merged["has_diabetes"] |
    merged["has_copd"] |
    merged["has_cancer"] |
    merged["has_hypertension"] |
    merged["has_kidney_disease"] |
    merged["has_stroke"] |
    merged["has_depression"]
).astype(int)

condition["has_chronic_term"] = condition["DESCRIPTION"].str.contains("chronic", na=False)
chronic_keyword_flag = condition.groupby("ENCOUNTER")["has_chronic_term"].max().reset_index()
merged = merged.merge(chronic_keyword_flag, left_on="VISIT_ID", right_on="ENCOUNTER", how="left").drop(columns=["ENCOUNTER"]).fillna(0)
merged["has_chronic_term"] = merged["has_chronic_term"].astype(int)

merged["has_chronic_condition"] = (
    merged["has_chronic_condition"] | merged["has_chronic_term"]
).astype(int)

# Select features
features = [
    "age_at_visit", "length_of_stay", "num_conditions", "num_unique_drugs",
    "num_procedures", "num_measurements"
] + [col for col in merged.columns if col.startswith("GENDER_") or col.startswith("RACE_") or col.startswith("CLASS_")] + \
  disease_flags + medication_flags + ["has_chronic_condition"]

final_df = merged[features + ["readmitted_30_days", "readmitted_60_days"]].dropna()

# Export dataset
final_df.to_csv("readmission_dataset.csv", index=False)
print("✅ Saved readmission_dataset.csv with shape:", final_df.shape)

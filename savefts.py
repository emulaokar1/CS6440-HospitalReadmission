import json
import pandas as pd

# Load dataset (relative path if in same directory)
df = pd.read_csv("readmission_dataset.csv")

# Drop label columns to get feature list
features = [col for col in df.columns if col not in ["readmitted_30_days", "readmitted_60_days"]]

# Save feature list to JSON
with open("features.json", "w") as f:
    json.dump(features, f)

print("✅ features.json saved with", len(features), "features.")

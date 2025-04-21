import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import joblib
import json
import shap

# Load models
model_30 = joblib.load("xgb/xgb_model_30_day.pkl")
model_60 = joblib.load("xgb/xgb_model_60_day.pkl")


# Load feature list
with open("features.json", "r") as f:
    features = json.load(f)

# Page configuration
st.set_page_config(
    page_title="Hospital Readmission Predictor",
    page_icon="🏥",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>

/* Global background */
.stApp {
    background-color: #f9f9f9;
}

/* Headings and labels */
h1, h2, h3, h4, h5, h6, label, .stMarkdown {
    color: #000 !important;
}

/* Input + textarea fields */
input[type="text"],
input[type="number"],
textarea,
.stTextInput input,
.stTextArea textarea,
input {
    color: white !important;
    background-color: #292b2f !important;
}

/* Dropdown menus and selected options */
div[role="listbox"],
div[role="combobox"],
div[role="option"],
li[role="option"],
[class*="singleValue"],
[role="combobox"] input[type="text"],
.stSelectbox *,
div[data-baseweb="select"] > div {
    color: white !important;
    background-color: #292b2f !important;
}

/* Number input (spin buttons) */
input[type="number"],
div[role="spinbutton"] input {
    color: #fff !important;
    background-color: #292b2f !important;
}

/* Buttons */
.stButton > button {
    background-color: #0078d4;
    color: #fff !important;
    border-radius: 8px;
    height: 3em;
}
.stButton > button:hover {
    background-color: #005a9e;
}

/* 🟩 Make st.success (green box) text black */
div.stAlert[data-testid="stAlert-success"],
div.stAlert[data-testid="stAlert-success"] * {
    color: black !important;
}

/* 🔵 Make st.info (blue box / disclaimer) text black */
div.stAlert[data-testid="stAlert-info"],
div.stAlert[data-testid="stAlert-info"] * {
    color: black !important;
}

</style>
""", unsafe_allow_html=True)


# App title and description
st.title("🏥 Hospital Readmission Prediction Tool")
st.markdown(
    "<p style='color:black;'>Use this interactive tool to estimate the likelihood of a patient’s readmission based on key demographic and clinical inputs.</p>",
    unsafe_allow_html=True
)

# Input form
with st.form("readmission_form"):
    st.header("Patient Information")

    # ── Demographics ──
    st.subheader("Demographics")
    col1, col2, col3 = st.columns(3)

    # Titles above each column
    with col1:
        st.markdown("**Age**")
        age = st.number_input("Age", min_value=0, max_value=120, value=30, help="Patient age in years")
        

    with col2:
        st.markdown("**Race**")
        race = st.selectbox(
            "Race",
            ["White", "Black", "Asian", "Hawaiian/Pacific Islander", "Alaskan/Native American", "Other"],
            help="Patient race", label_visibility="hidden"
        )

    with col3:
        st.markdown("**Gender**")
        gender = st.selectbox("Gender", ["Male", "Female"], help="Patient gender", label_visibility="hidden")
    
    st.markdown("---")

    # ── Clinical & Encounter Features ──
    st.subheader("Clinical/Encounter Features")
    length_of_stay    = st.number_input(
        "Length of stay (hrs)", min_value=0.0, value=24.0, step=0.5,
        help="Total duration of this encounter in hours"
    )
    num_conditions    = st.number_input(
        "Number of Conditions", min_value=0, value=0, help="Count of unique diagnosis codes"
    )
    num_unique_drugs  = st.number_input(
        "Number of Unique Drugs", min_value=0, value=0, help="Count of distinct medications"
    )
    num_procedures    = st.number_input(
        "Number of Procedures", min_value=0, value=0, help="Count of procedures performed"
    )
    num_measurements  = st.number_input(
        "Number of Measurements", min_value=0, value=0, help="Count of lab/vital measurements"
    )
   
    encounter_type = st.text_area(
        "Encounter Type", 
        placeholder="Ambulatory, Emergency, Inpatient, Outpatient, Wellness, Urgent Care, Home, Hospice, or SNF",
        help="Select the type of visit"
    )

    comorbidities_input = st.text_area(
    "Comorbidities (comma-separated)",
    placeholder="e.g., diabetes, COPD, heart failure",
    help="Enter known chronic conditions or comorbidities"
    )

    medications_input = st.text_area(
        "Medications (comma-separated)",
        placeholder="e.g., insulin, statins, metformin",
        help="List any medications the patient is currently on"
    )
    submitted = st.form_submit_button("Predict Readmission")

# Prediction and gauge display
if submitted:
    # ──────────────────────────────────────
    # 1) Initialize feature row
    # ──────────────────────────────────────
    row = {f: 0 for f in features}

    # Numeric + one‑hot you already have:
    row["age_at_visit"]      = age
    row["length_of_stay"]    = length_of_stay
    row["num_conditions"]    = num_conditions
    row["num_unique_drugs"]  = num_unique_drugs
    row["num_procedures"]    = num_procedures
    row["num_measurements"]  = num_measurements

    row["GENDER_M"] = 1 if gender == "Male" else 0
    row["GENDER_F"] = 1 if gender == "Female" else 0

    # map race -> one‑hot exactly as in your FEATURES list
    race_map = {
        "White":     "RACE_white",
        "Black":     "RACE_black",
        "Asian":     "RACE_asian",
        "Hawaiian/Pacific Islander": "RACE_hawaiian",
        "Alaskan/Native American":    "RACE_native",
        "Other":     "RACE_other"
    }
    row[race_map.get(race, "RACE_other")] = 1

    encounter_map = {
    "Ambulatory": "CLASS_ambulatory",
    "Emergency": "CLASS_emergency",
    "Inpatient": "CLASS_inpatient",
    "Outpatient": "CLASS_outpatient",
    "Wellness": "CLASS_wellness",
    "Urgent Care": "CLASS_urgentcare",
    "Home": "CLASS_home",
    "Hospice": "CLASS_hospice",
    "SNF": "CLASS_snf"
    }
    selected_class_feature = encounter_map.get(encounter_type)
    if selected_class_feature in row:
        row[selected_class_feature] = 1


    # ──────────────────────────────────────
    # 2) Parse comorbidities_input text
    # ──────────────────────────────────────
    text = comorbidities_input.lower()
    # disease patterns must match what you used in preprocessing
    disease_patterns = {
        "has_heart_disease":      ["heart", "coronary", "myocardial", "cvd", "angina", "mi"],
        "has_diabetes":           ["diabetes"],
        "has_copd":               ["copd", "bronchitis", "emphysema"],
        "has_cancer":             ["cancer", "carcinoma", "tumor", "malignancy", "neoplasm"],
        "has_hypertension":       ["hypertension", "htn", "high blood pressure"],
        "has_kidney_disease":     ["kidney", "renal failure", "ckd"],
        "has_stroke":             ["stroke", "cva", "cerebrovascular"],
        "has_depression":         ["depression", "depressive", "mood disorder"],
    }
    for flag, keywords in disease_patterns.items():
        if any(kw in text for kw in keywords):
            row[flag] = 1

    # ──────────────────────────────────────
    # 3) Parse medications_input text
    # ──────────────────────────────────────
    med_text = medications_input.lower()
    med_patterns = {
        "is_opioid":          ["morphine", "fentanyl", "oxycodone", "hydrocodone"],
        "is_insulin":         ["insulin"],
        "is_anticoagulant":   ["warfarin", "heparin", "apixaban", "xarelto"],
        "is_antipsychotic":   ["haloperidol", "risperidone", "olanzapine"],
        "is_antibiotic":      ["penicillin", "amoxicillin", "azithromycin"],
        "is_statins":         ["atorvastatin", "simvastatin"],
    }
    for flag, keywords in med_patterns.items():
        if any(kw in med_text for kw in keywords):
            row[flag] = 1

    # ──────────────────────────────────────
    # 4) Chronic summary (optional if you kept it)
    # ──────────────────────────────────────
    row["has_chronic_condition"] = int(
        any(row[flag] == 1 for flag in disease_patterns.keys())
    )

    # ──────────────────────────────────────
    # 5) Build DataFrame & predict
    # ──────────────────────────────────────
    input_df = pd.DataFrame([row], columns=features)

    p30 = model_30.predict_proba(input_df)[0,1]
    p60 = model_60.predict_proba(input_df)[0,1]

    # 30-day risk box
    st.markdown(f"""
    <div style='
        background-color: #d4edda;
        padding: 1em;
        margin-top: 1em;
        margin-bottom: 1em;
        border-radius: 10px;
        border: 1px solid #c3e6cb;
        color: black;
    '>
    ✅ <strong>30‑day readmission risk:</strong> {p30:.1%}
    </div>
    """, unsafe_allow_html=True)

    # 60-day risk box
    st.markdown(f"""
    <div style='
        background-color: #d4edda;
        padding: 1em;
        margin-bottom: 1em;
        border-radius: 10px;
        border: 1px solid #c3e6cb;
        color: black;
    '>
    ✅ <strong>60‑day readmission risk:</strong> {p60:.1%}
    </div>
    """, unsafe_allow_html=True)

    # Determine bold color based on thresholds
    def get_color_30(p):
        if p < 0.33:
            return "#27ae60"   # green
        elif p < 0.67:
            return "#f1c40f"   # yellow
        else:
            return "#e74c3c"   # red

    def get_color_60(p):
        if p < 0.33:
            return "#27ae60"   # green
        elif p < 0.67:
            return "#f1c40f"   # yellow
        else:
            return "#e74c3c"   # red

    # Row of two vertical sections
    col1, col2 = st.columns(2)

    # ──────────────── 30‑Day ────────────────
    with col1:
        # Gauge
        fill_color_30 = get_color_30(p30)
        fig, ax = plt.subplots(figsize=(2, 2))
        ax.pie(
            [p30, 1 - p30],
            startangle=90,
            colors=[fill_color_30, "#e3e3e3"],
            wedgeprops={"width": 0.25, "edgecolor": "white"}
        )
        ax.text(0, 0, f"{p30:.1%}", ha="center", va="center", fontsize=16, weight="bold")
        ax.axis("equal")
        st.pyplot(fig, use_container_width=False)
        st.caption("📅 30‑Day Readmission Risk")

        # SHAP
        st.caption("SHAP Explanation (30-Day)")
        explainer_30 = shap.Explainer(model_30)
        shap_vals_30 = explainer_30(input_df)
        fig, ax = plt.subplots(figsize=(8, 5))  # wider plot
        shap.plots.bar(shap_vals_30[0], show=False, ax=ax)
        ax.tick_params(axis='y', labelsize=9)  # smaller y-axis font
        st.pyplot(fig)


    # ──────────────── 60‑Day ────────────────
    with col2:
        # Gauge
        fill_color_60 = get_color_60(p60)
        fig, ax = plt.subplots(figsize=(2, 2))
        ax.pie(
            [p60, 1 - p60],
            startangle=90,
            colors=[fill_color_60, "#e3e3e3"],
            wedgeprops={"width": 0.25, "edgecolor": "white"}
        )
        ax.text(0, 0, f"{p60:.1%}", ha="center", va="center", fontsize=16, weight="bold")
        ax.axis("equal")
        st.pyplot(fig, use_container_width=False)
        st.caption("📆 60‑Day Readmission Risk")

        # SHAP
        st.caption("SHAP Explanation (60-Day)")
        explainer_60 = shap.Explainer(model_60)
        shap_vals_60 = explainer_60(input_df)
        fig, ax = plt.subplots(figsize=(8, 5))  # wider plot
        shap.plots.bar(shap_vals_30[0], show=False, ax=ax)
        ax.tick_params(axis='y', labelsize=9)  # smaller y-axis font
        st.pyplot(fig)


    # Final note
    # Disclaimer
    st.markdown("""
    <div style='
        background-color: #d1ecf1;
        padding: 1em;
        margin-top: 2em;
        border-radius: 10px;
        border: 1px solid #bee5eb;
        color: black;
    '>
    ℹ️ <strong>Disclaimer:</strong> This is a predictive model and should not be used for medical decision-making without consultation from healthcare professionals.
    </div>
    """, unsafe_allow_html=True)
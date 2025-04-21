import streamlit as st
import numpy as np
import matplotlib.pyplot as plt

# Page configuration
st.set_page_config(
    page_title="Hospital Readmission Predictor",
    page_icon="🏥",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
    <style>
    /* Global text color */
    .stApp { background-color: #f9f9f9; }
    .stApp * { color: #000 }

    /* Style select inputs native and React-Select */
    select, select option * { color: #fff !important; background-color: #292b2f !important; }
    input * {color: #fff !important;}
    [role="listbox"], div[role="listbox"] * { color: white !important; }
    div[role="option"], li[role="option"] { color: white !important; }

    /* Ensure selected value (singleValue) in React-Select is white */
    [class*="singleValue"] { color: #fff !important; }
    /* Also target the internal input element */
    [role="combobox"] input[type="text"] { color: #fff !important; background-color: #292b2f !important; }
    .stSelectbox * {color: white !important;}
    div[data-baseweb="select"] > div {color: #fff !important;}
    input {color: #fff !important;}
    /* Style number inputs (spinbuttons) */
    input[type="number"], div[role="spinbutton"] input { color: #fff !important; background-color: #292b2f !important; }

    /* Button styling */
    .stButton>button { background-color: #0078d4; color: #fff !important; border-radius: 8px; height: 3em; }
    .stButton>button:hover { background-color: #005a9e; }
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
    st.subheader("Demographics")
    col1, col2, col3 = st.columns(3)
    with col1:
        age = st.number_input("Age", min_value=0, max_value=120, value=30, help="Patient age in years")
        gender = st.selectbox("Gender", ["Male", "Female"], help="Patient gender")
    with col2:
        race = st.selectbox(
            "Race",
            ["White", "Black", "Asian", "Hawaiian/Pacific Islander", "Alaskan/Native American"],
            help="Patient race"
        )
        ethnicity = st.selectbox("Ethnicity", ["Hispanic or Latino", "Not Hispanic or Latino"], help="Patient ethnicity")
    with col3:
        observations = st.number_input("Number of Observations", min_value=0, value=0, help="Total observations collected")
        measurements = st.number_input("Number of Measurements", min_value=0, value=0, help="Total measurements taken")

    submitted = st.form_submit_button(label="Predict Readmission")

# Prediction and gauge display
if submitted:
    risk_score = np.random.uniform(0, 1)  # placeholder
    percent = risk_score * 100

    st.success(f"Prediction Complete! The patient has a {percent:.1f}% chance of being readmitted")

    # Determine bold color based on thresholds
    if percent < 30:
        fill_color = '#27ae60'  # bold green
    elif percent < 75:
        fill_color = '#f1c40f'  # bold yellow
    else:
        fill_color = '#e74c3c'  # bold red

    # Create a smaller gauge chart
    fig, ax = plt.subplots(figsize=(2, 2))
    fig.set_size_inches(2,2)
    ax.pie(
        [percent, 100 - percent],
        startangle=90,
        colors=[fill_color, '#e3e3e3'],
        wedgeprops={'width': 0.25, 'edgecolor': 'white'}
    )
    ax.text(0, 0, f"{percent:.1f}%", ha='center', va='center', fontsize=16, weight='bold')
    ax.axis('equal')

    st.pyplot(fig, use_container_width=False)
    st.info("Disclaimer: This is just a prediction")

import streamlit as st

st.title("Hospital Readmission Prediction")

st.header("Enter your info below:")

# Input fields
gender = st.text_input("Gender")
age = st.number_input(" Age", min_value=0, max_value=120)

# Submit button
if st.button("Submit"):
    st.success(f"Patient is {age} years old")

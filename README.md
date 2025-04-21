# Hospital Readmission Model
Use CSV files from the zipped folder. The csv_files folder needs to be in the same directory as main.py. It needs to have the 6 csv files in it.
Adjust the csv_dir filepath as necessary for you and your file structure, it's probably not the same as mine

# Running Streamlit app:
Navigate to folder and run:
pip install --only-binary=:all: pyarrow

Then:
pip install streamlit

pip install joblib
pip install shap
pip install xgboost

Then:
streamlit run app.py


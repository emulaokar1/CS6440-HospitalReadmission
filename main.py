from util.sqlalchemydb import create_connection
from util.OmopOnFhir import t_f_person, t_measurement, t_person, t_observation
from sqlalchemy.orm import Session
from sqlalchemy import text, select, create_engine
from pyomop import CdmEngineFactory, CdmVocabulary, CdmVector, Cohort, Vocabulary, metadata
from sqlalchemy.future import select
import datetime
from student_code import get_measurement_value, get_patient_name, get_patient_gender_as_string
import asyncio
import os
import csv
import hashlib
# Modify your database connection string as needed.
database_connection_string = 'postgresql://postgres:password@localhost:5434/omop54'

def main():
    engine = create_connection(database_connection_string, True)
    # Set the directory where your CSV files are located.
    csv_dir = "student/csv_files"  # Adjust this path as necessary.

    # Define the CSV file paths.
    patients_csv = os.path.join(csv_dir, "patients.csv")
    encounters_csv = os.path.join(csv_dir, "encounters.csv")
    conditions_csv = os.path.join(csv_dir, "conditions.csv")
    observations_csv = os.path.join(csv_dir, "observations.csv")
    medications_csv = os.path.join(csv_dir, "medications.csv")
    procedures_csv = os.path.join(csv_dir, "procedures.csv")

    # Transform each CSV file into OMOP CDM–compatible data.
    patients = etl_patients(patients_csv)
    encounters = etl_encounters(encounters_csv)
    conditions = etl_conditions(conditions_csv)
    observations = etl_observations(observations_csv)
    medications = etl_medications(medications_csv)
    procedures = etl_procedures(procedures_csv)
    # print("START OF EXERCISE 1 SQLALCHEMY------------------------------------------------")
    # call_exercise_1_using_sqlalchemy(engine)
    # print("START OF EXERCISE 1 SQL----------------------------------------------")
    # call_exercise_1_using_sql_example(engine)
    # print("START OF EXERCISE 2 ------------------------------------------------")
    # call_exercise_2(engine)
    #call_exercise_3(engine)
    insert_omop_data(engine, patients, encounters, conditions, observations, medications, procedures)
  

def get_id_from_uuid(uuid_str, mod_value=None):
    hash_hex = hashlib.md5(uuid_str.encode('utf-8')).hexdigest()
    hash_int = int(hash_hex, 16)
    if mod_value:
        return hash_int % mod_value
    return hash_int
def generate_condition_occurrence_id(person_id, visit_occurrence_id, condition_code):
    unique_str = f"{person_id}-{visit_occurrence_id}-{condition_code}"
    hash_hex = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
    hash_int = int(hash_hex, 16)
    return hash_int % (2**31)
def observation_code_to_int(code_str, mod_value=2**31):
    cleaned = code_str.replace('-', '')
    if cleaned.isdigit():
        return int(cleaned)
    else:
        hash_hex = hashlib.md5(code_str.encode('utf-8')).hexdigest()
        hash_int = int(hash_hex, 16)
        return hash_int % mod_value
def generate_drug_exposure_id(patient, start, encounter, code, mod_value=2**31):
    unique_str = f"{patient}-{start}-{encounter}-{code}"
    hash_hex = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
    hash_int = int(hash_hex, 16)
    return hash_int % mod_value
def generate_procedure_occurrence_id(patient, start, encounter, code, mod_value=2**31):
    unique_str = f"{patient}-{start}-{encounter}-{code}"
    hash_hex = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
    hash_int = int(hash_hex, 16)
    return hash_int % mod_value

def get_race_concept_id(row):
    if row["RACE"]:
        race_val = row["RACE"].lower()
        if "white" in race_val:
            return 8527
        elif "black" in race_val or "african american" in race_val:
            return 8516
        elif "asian" in race_val:
            return 8515
        elif "hawaiian" in race_val or "islander" in race_val:
            return 8557
        elif "alaska" in race_val or "american indian" in race_val:
            return 8517
    return 8552 #unknown code
# read each CSV and transform rows into dictionaries that match OMOP tables.
def etl_patients(csv_file):
    patients = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ethnicity_source = None
            ethnicity_code = 0
            if row["ETHNICITY"] and row["ETHNICITY"] == "nonhispanic":
                ethnicity_source = "Not Hispanic or Latino"
                ethnicity_code = 38003564
            elif row["ETHNICITY"] and row["ETHNICITY"] == "hispanic":
                ethnicity_source = "Hispanic or Latino"
                ethnicity_code = 38003563
            patients.append({
                "person_id": get_id_from_uuid(row["Id"], mod_value=10**9),
                "person_source_value": row["Id"],
                "gender_concept_id": 8507 if row["GENDER"] == "M" else 8532,
                "gender_source_value": row["GENDER"] if row["GENDER"] else None,
                "year_of_birth": int(row["BIRTHDATE"][:4]) if row["BIRTHDATE"] else None,
                "month_of_birth": int(row["BIRTHDATE"].split('-')[1]) if row["BIRTHDATE"] else None,
                "day_of_birth": int(row["BIRTHDATE"].split('-')[2]) if row["BIRTHDATE"] else None,
                "race_concept_id": get_race_concept_id(row),
                "race_source_value": row["RACE"] if row["RACE"] else None,
                "ethnicity_source_value": ethnicity_source,
                "ethnicity_concept_id": ethnicity_code,
                "ethnicity_source_concept_id": ethnicity_code
            })
    return patients

def etl_encounters(csv_file):
    encounters = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            encounters.append({
                "visit_occurrence_id": get_id_from_uuid(row["Id"], mod_value=10**9),
                "person_id": get_id_from_uuid(row["PATIENT"], mod_value=10**9),
                "visit_start_date": row["START"][:10],
                "visit_end_date": row["STOP"][:10],
                "visit_concept_id": int(row["CODE"]) % 2**31,  # Replace with the appropriate OMOP concept id if needed.
                "visit_type_concept_id": int(row["CODE"]) % 2**31,
                "visit_source_value": row["ENCOUNTERCLASS"],
                "visit_source_concept_id": int(row["REASONCODE"]) % 2**31 if row["REASONCODE"] else None
            })
    return encounters

def etl_conditions(csv_file):
    conditions = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            conditions.append({
                "condition_occurrence_id": generate_condition_occurrence_id(row["PATIENT"], row["ENCOUNTER"], row["CODE"]),
                "person_id": get_id_from_uuid(row["PATIENT"], mod_value=10**9),
                "condition_concept_id": int(row["CODE"]) % 2**31,  # Map this to the appropriate OMOP concept id.
                "condition_start_date": row["START"],
                "condition_end_date": row["STOP"] if row["STOP"] else None,
                "visit_occurrence_id": get_id_from_uuid(row["ENCOUNTER"], mod_value=10**9),
                "condition_source_value": row["DESCRIPTION"][:50],
                "condition_type_concept_id": int(row["CODE"]) % 2**31
                
            })
    return conditions

def etl_observations(csv_file):
    observations = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            observations.append({
                "measurement_id": generate_condition_occurrence_id(row["PATIENT"], row["ENCOUNTER"], row["CODE"]),
                "person_id": get_id_from_uuid(row["PATIENT"], mod_value=10**9),
                "measurement_concept_id": observation_code_to_int(row["CODE"]),  # Map the observation code appropriately.
                "measurement_type_concept_id": observation_code_to_int(row["CODE"]),
                "value_as_number": row["VALUE"] if row["TYPE"] == "numeric" else None,
                "measurement_date": row["DATE"][:10],
                "value_source_value": row["VALUE"][:50] if row["VALUE"] else None,
                "unit_source_value": row["UNITS"] if row["UNITS"] else None,
            })
    return observations

def etl_medications(csv_file):
    medications = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
           
            medications.append({
                "drug_exposure_id": generate_drug_exposure_id(row["PATIENT"], row["START"], row["ENCOUNTER"], row["CODE"], mod_value=2**31),
                "person_id": get_id_from_uuid(row["PATIENT"], mod_value=10**9),
                "drug_concept_id": int(row["CODE"]),  # Map this as needed for OMOP.
                "drug_exposure_start_date": row["START"][:10],
                "drug_exposure_end_date": row["STOP"][:10] if row["STOP"] else "9999-12-12",
                "visit_occurrence_id": get_id_from_uuid(row["ENCOUNTER"], mod_value=10**9),
                "drug_source_value": row["DESCRIPTION"][:50],
                "drug_type_concept_id": int(row["REASONCODE"]) % 2**31 if row["REASONCODE"] else 0,
                "quantity": int(row["DISPENSES"]),
                "route_source_value": row["REASONDESCRIPTION"][:50]
            })
    return medications

def etl_procedures(csv_file):
    procedures = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            procedures.append({
                "procedure_occurrence_id": generate_procedure_occurrence_id(row["PATIENT"], row["START"], row["ENCOUNTER"], row["CODE"], mod_value=2**31),
                "person_id": get_id_from_uuid(row["PATIENT"], mod_value=10**9),
                "procedure_concept_id": int(row["CODE"]) % 2**31,  # Map this code to an OMOP concept id.
                "procedure_date": row["START"][:10],
                "procedure_end_date":row["STOP"][:10],
                "visit_occurrence_id": get_id_from_uuid(row["ENCOUNTER"], mod_value=10**9),
                "procedure_type_concept_id": int(row["CODE"]) % 2**31,
                "procedure_source_value": row["DESCRIPTION"][:50],
                "modifier_concept_id": int(row["REASONCODE"]) % 2**31 if row["REASONCODE"] else 0
            })
    return procedures

#nsert the transformed OMOP data into the omop54 database.
def insert_omop_data(engine, patients, encounters, conditions, observations, medications, procedures):
    with Session(engine) as session:
        # Insert patients into the person table.
        for p in patients:
            sql = text("""
                            INSERT INTO person (
                                person_id,
                                person_source_value,
                                gender_concept_id,
                                gender_source_value,
                                year_of_birth,
                                month_of_birth,
                                day_of_birth,
                                race_concept_id,
                                race_source_value,
                                ethnicity_source_value,
                                ethnicity_concept_id,
                                ethnicity_source_concept_id
                            )
                            VALUES (
                                :person_id,
                                :person_source_value,
                                :gender_concept_id,
                                :gender_source_value,
                                :year_of_birth,
                                :month_of_birth,
                                :day_of_birth,
                                :race_concept_id,
                                :race_source_value,
                                :ethnicity_source_value,
                                :ethnicity_concept_id,
                                :ethnicity_source_concept_id
                            );
                        """)

            params = {
                "person_id": p["person_id"],
                "person_source_value": p["person_source_value"],
                "gender_concept_id": p["gender_concept_id"],
                "gender_source_value": p["gender_source_value"],
                "year_of_birth": p["year_of_birth"],
                "month_of_birth": p["month_of_birth"],
                "day_of_birth": p["day_of_birth"],
                "race_concept_id": p["race_concept_id"],
                "race_source_value": p["race_source_value"],
                "ethnicity_source_value": p["ethnicity_source_value"],
                "ethnicity_concept_id": p["ethnicity_concept_id"],
                "ethnicity_source_concept_id": p["ethnicity_source_concept_id"]
            }
            # sql = text(f"""DELETE FROM person; """)
            session.execute(sql, params)
        # Insert encounters into visit_occurrence.
        for e in encounters:
            sql = text(f"""
                INSERT INTO visit_occurrence ( "visit_occurrence_id",
                "person_id",
                "visit_start_date",
                "visit_end_date",
                "visit_concept_id",
                "visit_type_concept_id",
                "visit_source_value",
                "visit_source_concept_id")
                VALUES (:visit_occurrence_id,
                        :person_id,
                        :visit_start_date,
                        :visit_end_date,
                        :visit_concept_id,
                        :visit_type_concept_id,
                        :visit_source_value,
                        :visit_source_concept_id);
                """)
            params = {
                'visit_occurrence_id': e['visit_occurrence_id'],
                'person_id': e['person_id'],
                'visit_start_date': e['visit_start_date'],
                'visit_end_date': e['visit_end_date'],
                'visit_concept_id': e['visit_concept_id'],
                'visit_type_concept_id': e['visit_type_concept_id'],
                'visit_source_value': e['visit_source_value'],
                'visit_source_concept_id': e.get('visit_source_concept_id')  # can be None
            }
            #  sql = text(f"""DELETE FROM visit_occurrence; """)
            session.execute(sql, params)
        # # Insert conditions into condition_occurrence.
        for c in conditions:
            sql = text(f"""
                INSERT INTO condition_occurrence ("condition_occurrence_id",
                "person_id",
                "condition_concept_id",
                "condition_start_date",
                "condition_end_date",
                "visit_occurrence_id",
                "condition_source_value",
                "condition_type_concept_id")
                VALUES (:condition_occurrence_id,
                        :person_id,
                        :condition_concept_id,
                        :condition_start_date,
                        :condition_end_date,
                        :visit_occurrence_id,
                        :condition_source_value,
                        :condition_type_concept_id);
            """)
            params = {"condition_occurrence_id": c["condition_occurrence_id"],
                "person_id": c["person_id"],
                "condition_concept_id": c["condition_concept_id"],  
                "condition_start_date": c["condition_start_date"],
                "condition_end_date": c["condition_end_date"],
                "visit_occurrence_id": c["visit_occurrence_id"],
                "condition_source_value": c["condition_source_value"],
                "condition_type_concept_id": c["condition_type_concept_id"]}
            #  sql = text(f"""DELETE FROM condition_occurrence; """)
            session.execute(sql, params)
        # # Insert observations into measurement.
        for o in observations:
            sql = text(f"""
                INSERT INTO measurement ("measurement_id",
                "person_id",
                "measurement_concept_id",
                "measurement_type_concept_id",
                "value_as_number",
                "measurement_date",
                "value_source_value",
                "unit_source_value")
                VALUES (:measurement_id,
                :person_id,
                :measurement_concept_id,
                :measurement_type_concept_id,
                :value_as_number,
                :measurement_date,
                :value_source_value,
                :unit_source_value);
            """)
            params = {"measurement_id": o["measurement_id"],
                "person_id": o["person_id"],
                "measurement_concept_id": o["measurement_concept_id"],
                "measurement_type_concept_id": o["measurement_type_concept_id"],
                "value_as_number": o["value_as_number"],
                "measurement_date": o["measurement_date"],
                "value_source_value": o["value_source_value"],
                "unit_source_value": o["unit_source_value"]}
            #  sql = text(f"""DELETE FROM measurement; """)
            session.execute(sql, params)
        # # Insert medications into drug_exposure.
        for m in medications:
            sql = text(f"""
                INSERT INTO drug_exposure ("drug_exposure_id",
                "person_id",
                "drug_concept_id",
                "drug_exposure_start_date",
                "drug_exposure_end_date",
                "visit_occurrence_id",
                "drug_source_value",
                "drug_type_concept_id",
                "quantity",
                "route_source_value")
                VALUES (:drug_exposure_id,
                :person_id,
                :drug_concept_id,
                :drug_exposure_start_date,
                :drug_exposure_end_date,
                :visit_occurrence_id,
                :drug_source_value,
                :drug_type_concept_id,
                :quantity,
                :route_source_value);
            """)
            params = {"drug_exposure_id": m['drug_exposure_id'],
                "person_id": m['person_id'],
                "drug_concept_id": m['drug_concept_id'],
                "drug_exposure_start_date": m['drug_exposure_start_date'],
                "drug_exposure_end_date": m['drug_exposure_end_date'],
                "visit_occurrence_id": m['visit_occurrence_id'],
                "drug_source_value": m['drug_source_value'],
                "drug_type_concept_id": m['drug_type_concept_id'],
                "quantity": m['quantity'],
                "route_source_value": m['route_source_value']}
            #  sql = text(f"""DELETE FROM drug_exposure; """)
            session.execute(sql, params)
        # # Insert procedures into procedure_occurrence.
        for pr in procedures:
            sql = text(f"""
                INSERT INTO procedure_occurrence ( "procedure_occurrence_id",
                "person_id",
                "procedure_concept_id",
                "procedure_date",
                "procedure_end_date",
                "visit_occurrence_id",
                "procedure_type_concept_id",
                "procedure_source_value",
                "modifier_concept_id")
                VALUES ( :procedure_occurrence_id,
                :person_id,
                :procedure_concept_id,
                :procedure_date,
                :procedure_end_date,
                :visit_occurrence_id,
                :procedure_type_concept_id,
                :procedure_source_value,
                :modifier_concept_id);
            """)
            params = {"procedure_occurrence_id": pr['procedure_occurrence_id'],
                "person_id": pr['person_id'],
                "procedure_concept_id": pr['procedure_concept_id'],
                "procedure_date": pr['procedure_date'],
                "procedure_end_date": pr["procedure_end_date"],
                "visit_occurrence_id": pr['visit_occurrence_id'],
                "procedure_type_concept_id": pr["procedure_type_concept_id"],
                "procedure_source_value": pr['procedure_source_value'],
                "modifier_concept_id": pr['modifier_concept_id']}
            # sql = text(f"""DELETE FROM procedure_occurrence; """)
            session.execute(sql, params)
        session.commit()
        print("OMOP data successfully inserted.")

# '''
# Two examples are provided below, the first using a standard SQL statement. The second using the SQL
# Alchemy models more directly. Either approach may be used.
# '''
# def call_exercise_1_using_sql_example(engine):
#     with Session(engine) as session:
#         statement = f'SELECT * FROM measurement;'
#         result = session.execute(text(statement))
#         print(f'TYPE : {type(result)}')
#         for row in result.all():
#             print(f'TYPE2: {type(row)}')
#             print(row._mapping)


# def call_exercise_1_using_sqlalchemy(engine):
#     with Session(engine) as session:
#         statement = select(t_person)
#         result = session.execute(statement)
#         for row in result.all():
#            print(row._mapping)


# def call_exercise_2(engine):
#     with Session(engine) as session:
#         statement = select(t_observation)
#         result = session.execute(statement)
#         for row in result.all():
#             print(row)

# '''
# The following exercise requires performing a JOIN on two tables, which will align the tables along a single column. To make this
# easier for those who are not familiar with SQL, the bulk of the SQL statement is given for you. You should only need to edit the variables
# at the top of the function in alignment with the OMOP CDM schema.

# An example of a full JOIN statement performed on the person and f_person tables:
# >>> SELECT * FROM person INNER JOIN f_person ON person.person_id=f_person.person_id;

# Note additionally the inclusion of the "result.keys()" usage. This allows you to access the column descriptions from the query itself,
# not using the local models provided. For this particular case, as a JOIN can produce any number of columns in any order, pulling this
# from the result will help align the data with the request more directly. It will be used in the exercise_3 function to dynamically
# determine the index of the column requested. Please read the description given in the student_code.py file for more information before
# writing any code here.
# '''
# def call_exercise_3(engine):
#     columns = "*"               # The columns you wish return. You can leave this as * if you would like.
#     second_table = "f_person"           # The name of the second table.
#     foreign_key = "person_id"            # The name of the foreign key in the person table on which to join with the second table.
#     foreign_key_equivalent = "f_person.person_id" # The name of the column in the second table which lines up with the person table's foreign key.

#     with Session(engine) as session:
#         statement = f'SELECT {columns} FROM person INNER JOIN {second_table} ON person.{foreign_key}={second_table}.{foreign_key_equivalent};'
#         result = session.execute(text(statement))
#         keys = result.keys()
#         for row in result.all():
#             print(get_patient_gender_as_string(row, keys))


if __name__ == '__main__':
    main()

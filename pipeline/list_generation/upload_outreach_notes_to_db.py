import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def write_outreach_notes_to_db(dirname, timestamp):
    print(dirname)
    engine = create_engine("postgresql:///?service=acdhs_housing")
    files = os.listdir(os.path.join("/mnt/data/projects/acdhs_housing/outreach_spreadsheets/saved_notes", dirname))
    for filename in files:
        print(filename)
        filepath = os.path.join("/mnt/data/projects/acdhs_housing/outreach_spreadsheets/saved_notes", dirname, filename)
        sheets_df = pd.read_excel(filepath, sheet_name=None) # all sheets/tabs
        for sheet_name in sheets_df: # iterate over orgs
            if sheet_name == 'full_list':
                # print("skipping...")
                continue
            print(sheet_name)
            cols_to_save = ['model_id', 'client_hash', 'as_of_date', 'spreadsheet_date', 'prediction_date', 'Attempt 1 notes', 'Attempt 2 notes', 'Attempt 3 notes', 'Contact status', 'Court status', 'Rental assistance status', 'Did this help tenant?']
            sheets_df[sheet_name] = sheets_df[sheet_name][cols_to_save]
            # clean status columns
            sheets_df[sheet_name]["Contact status"] = sheets_df[sheet_name].apply(clean_contact_status, axis=1)
            sheets_df[sheet_name]["Court status"] = sheets_df[sheet_name].apply(clean_court_status, axis=1)
            sheets_df[sheet_name]["Rental assistance status"] = sheets_df[sheet_name].apply(clean_ra_status, axis=1)
            sheets_df[sheet_name]["Did this help tenant?"] = sheets_df[sheet_name].apply(clean_help_status, axis=1)
            # add columns for db
            sheets_df[sheet_name]['data_source'] = sheet_name
            sheets_df[sheet_name]['db_recorded_timestamp'] = timestamp
            # clean up date columns
            date_cols = ['as_of_date', 'prediction_date', 'spreadsheet_date']
            for date_col in date_cols:
                sheets_df[sheet_name][date_col] = sheets_df[sheet_name][date_col].dt.date
            sheets_df[sheet_name].to_sql('outreach_contact_notes', engine, index=False, if_exists='append', schema='acdhs_production')
            print("wrote to db")
    print("recorded all files notes in db")

def clean_contact_status(row):
    if not pd.isnull(row["Contact status"]) and row["Contact status"] not in ('Provider contacted', 'Attempted', 'Unable to contact', 'Contacted', 'No need to contact', 'Reached'):
        print(f"Warning: setting Contact status to null from {row['Contact status']}")
        print(row)
        status = pd.NA        
    else:
        status = row['Contact status']
    return status

def clean_court_status(row):
    if not pd.isnull(row["Court status"]) and row["Court status"] not in ("Didn't appear", "Appeared & didn't make contact", "Appeared & made contact"):
        print(f"Warning: setting Court status to null from {row['Court status']}")
        print(row)
        status = pd.NA
    else:
        status = row["Court status"]
    return status

def clean_ra_status(row):
    if not pd.isnull(row["Rental assistance status"]) and row["Rental assistance status"] not in ('Unable to contact', "Not eligible/can't help", 'Application in progress', 'Payment made/approved', 'Not in Allegheny County'):
        print(f"Warning: setting Rental assistance status to null from {row['Rental assistance status']}")
        print(row)
        status = pd.NA
    else:
        status = row["Rental assistance status"]
    return status

def clean_help_status(row):
    if not pd.isnull(row["Did this help tenant?"]) and row["Did this help tenant?"] not in ('Yes--created new application', 'Yes--prioritized existing application', 'No', 'Yes'):
        print(f"Warning: setting Did this help tenant? status to null from {row['Did this help tenant?']}")
        print(row)
        status = pd.NA
    else:
        status = row["Did this help tenant?"]
    return status

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-d",
        "--dirname",
        type=str,
        help='name of directory that contains the notes to be saved (not full path)',
        required=True
    )
    args = parser.parse_args()
    timestamp = pd.Timestamp.now()
    df = write_outreach_notes_to_db(args.dirname, timestamp)
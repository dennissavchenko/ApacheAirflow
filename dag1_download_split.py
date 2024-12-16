from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import gspread
from gspread import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
import io
from sklearn.model_selection import train_test_split


# Function to download dataset
def download_dataset(ti):
    # Load the dataset
    df = pd.read_csv('/Users/dennissavchenko/Documents/uni/5/PRO1/ApacheAirflow/usa_house_prices.csv')
    ti.xcom_push(key='dataset', value=df.to_csv(index=False))


# Function to split dataset
def split_and_upload_to_sheets(ti):
    # Retrieve dataset
    dataset_csv = ti.xcom_pull(key='dataset', task_ids='download_data')
    df = pd.read_csv(io.StringIO(dataset_csv))

    # Split dataset into training and fine-tuning sets
    training_set, fine_tuning_set = train_test_split(df, test_size=0.2, random_state=42)

    # Google Sheets authentication
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/Users/dennissavchenko/Documents/uni/5/PRO1/ApacheAirflow/tutorx1-444910-f9fc7591a720.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet by name
    try:
        sheet = client.open('TutorX1')

        # Rename or create the first worksheet (tab) for Training Dataset
        try:
            worksheet_1 = sheet.get_worksheet(0)  # Get the first worksheet
            worksheet_1.update_title("Training Dataset")  # Rename it
        except WorksheetNotFound:
            worksheet_1 = sheet.add_worksheet(title="Training Dataset", rows=10000, cols=20)  # Create new tab
        worksheet_1.clear()  # Clear any existing data
        worksheet_1.update([training_set.columns.values.tolist()] + training_set.values.tolist())

        # Rename or create the second worksheet (tab) for Fine-Tuning Dataset
        try:
            worksheet_2 = sheet.get_worksheet(1)  # Get the second worksheet
            worksheet_2.update_title("Fine-Tuning Dataset")  # Rename it
        except WorksheetNotFound:
            worksheet_2 = sheet.add_worksheet(title="Fine-Tuning Dataset", rows=10000, cols=20)  # Create new tab
        worksheet_2.clear()  # Clear any existing data
        worksheet_2.update([fine_tuning_set.columns.values.tolist()] + fine_tuning_set.values.tolist())
    except gspread.SpreadsheetNotFound:
        print("Spreadsheet not found. Please create a Google Sheet named 'TutorX1' and try again.")


# DAG Definition
with DAG(
        'dag_1_download_split',
        default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)},
        schedule=None,
        start_date=datetime(2023, 1, 1),
        catchup=False
) as dag:
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_dataset
    )

    split_and_upload = PythonOperator(
        task_id='split_and_upload',
        python_callable=split_and_upload_to_sheets
    )

    download_data >> split_and_upload

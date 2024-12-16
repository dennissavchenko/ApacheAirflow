from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import io


# Function to download dataset from Google Sheets
def download_dataset_from_sheets(ti):
    # Google Sheets authentication
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/Users/dennissavchenko/Documents/uni/5/PRO1/ApacheAirflow/tutorx1-444910-f9fc7591a720.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet and download the dataset
    sheet = client.open('TutorX1')
    worksheet = sheet.worksheet("Training Dataset")  # Assuming we're working with the Training Dataset
    data = worksheet.get_all_values()

    # Convert to DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])  # First row is header
    ti.xcom_push(key='raw_dataset', value=df.to_csv(index=False))


# Function to clean data
def clean_data(ti):
    # Retrieve the raw dataset
    raw_dataset_csv = ti.xcom_pull(key='raw_dataset', task_ids='download_data_from_sheets')
    df = pd.read_csv(io.StringIO(raw_dataset_csv))

    # Data cleaning steps
    df = df.drop_duplicates()  # Remove duplicates
    df = df.fillna(method='ffill')  # Handle missing values (forward fill as an example)
    ti.xcom_push(key='cleaned_dataset', value=df.to_csv(index=False))


# Function to standardize and normalize data
def standardize_and_normalize_data(ti):
    # Retrieve the cleaned dataset
    cleaned_dataset_csv = ti.xcom_pull(key='cleaned_dataset', task_ids='clean_data')
    df = pd.read_csv(io.StringIO(cleaned_dataset_csv))

    # Standardization and Normalization
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    scaler = StandardScaler()
    normalizer = MinMaxScaler()

    df[numeric_columns] = scaler.fit_transform(df[numeric_columns])  # Standardization
    df[numeric_columns] = normalizer.fit_transform(df[numeric_columns])  # Normalization

    ti.xcom_push(key='processed_dataset', value=df.to_csv(index=False))


# Function to upload dataset back to Google Sheets
def upload_processed_dataset(ti):
    # Retrieve the processed dataset
    processed_dataset_csv = ti.xcom_pull(key='processed_dataset', task_ids='standardize_and_normalize_data')
    df = pd.read_csv(io.StringIO(processed_dataset_csv))

    # Google Sheets authentication
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/Users/dennissavchenko/Documents/uni/5/PRO1/ApacheAirflow/tutorx1-444910-f9fc7591a720.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet and upload the dataset
    sheet = client.open('TutorX1')
    try:
        worksheet = sheet.worksheet("Processed Dataset")
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title="Processed Dataset", rows=10000, cols=20)

    worksheet.clear()  # Clear any existing data
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


# DAG Definition
with DAG(
        'dag_2_data_processing',
        default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)},
        schedule=None,
        start_date=datetime(2023, 1, 1),
        catchup=False
) as dag:
    download_data_from_sheets = PythonOperator(
        task_id='download_data_from_sheets',
        python_callable=download_dataset_from_sheets
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    standardize_and_normalize_data = PythonOperator(
        task_id='standardize_and_normalize_data',
        python_callable=standardize_and_normalize_data
    )

    upload_processed_dataset = PythonOperator(
        task_id='upload_processed_dataset',
        python_callable=upload_processed_dataset
    )

    # Task dependencies
    download_data_from_sheets >> clean_data >> standardize_and_normalize_data >> upload_processed_dataset

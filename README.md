# Apache Airflow

This project consists of two Airflow DAGs designed to orchestrate a simple end-to-end data processing pipeline using Google Sheets as the storage medium:

1. **DAG 1: Data Download and Splitting**  
   - **Download Dataset**: Fetch data from a public source.
   - **Split Dataset**: Separate the dataset into training and fine-tuning portions.
   - **Upload to Google Sheets**: Store each split dataset in its own Google Sheet using OAuth 2.0 or service account authentication.

2. **DAG 2: Data Processing**  
   - **Download Training Data**: Retrieve the training dataset from its Google Sheet.
   - **Data Cleaning**: Handle missing values, remove or process duplicates.
   - **Standardization & Normalization**: Use `StandardScaler` and `MinMaxScaler` (scikit-learn) to transform feature values.
   - **Upload Processed Data**: Save the cleaned and processed dataset back to Google Sheets or another cloud location.

Each DAG includes well-documented tasks with clear dependencies. After deployment in Airflow, you can view DAG runs and logs in the Airflow UI to confirm successful execution. 

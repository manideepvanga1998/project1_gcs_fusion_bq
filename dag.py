from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['vishal.bulbule@techtrapture.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def my_task():
    import csv
    from faker import Faker
    import random
    import string
    from google.cloud import storage

# Specify number of employees to generate
    num_employees = 100

# Create Faker instance
    fake = Faker()

# Define the character set for the password
    password_characters = string.ascii_letters + string.digits + 'm'

# Generate employee data and save it to a CSV file
    with open('employee_data.csv', mode='w', newline='') as file:
        fieldnames = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    for _ in range(num_employees):
        writer.writerow({
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "job_title": fake.job(),
            "department": fake.job(),  # Generate department-like data using the job() method
            "email": fake.email(),
            "address": fake.city(),
            "phone_number": fake.phone_number(),
            "salary": fake.random_number(digits=5),  # Generate a random 5-digit salary
            "password": ''.join(random.choice(password_characters) for _ in range(8))  # Generate an 8-character password with 'm'
                })

# Upload the CSV file to a GCS bucket
    def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
        storage_client = storage.Client(project="prepare-projects")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Set your GCS bucket name and destination file name
    bucket_name = 'pythontogcs'
    source_file_name = 'employee_data.csv'
    destination_blob_name = 'employee_data.csv'

# Upload the CSV file to GCS
    upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

dag = DAG('python_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    task1 = PythonOperator(
        task_id="run_python_function",
        python_callable=my_task
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="us-central1",
    pipeline_name="first-pipeline",
    instance_name="storagetobq",
    task_id="start_datafusion_pipeline",
    )

    task1 >> start_pipeline
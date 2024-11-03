from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import pandas as pd
import boto3
import logging
import json
import csv
import requests

# Variables from Airflow UI (secured in Airflow variables)
FB_ACCESS_TOKEN = Variable.get("FB_ACCESS_TOKEN", deserialize_json=True, default_var={}).get("access_token")
FACEBOOK_BASE_API_URL = Variable.get("FACEBOOK_BASE_API_URL")
FB_CATALOG_ID = Variable.get("FB_CATALOG_ID")

# Set up logging for monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# S3 Constants
BUCKET_NAME = 'dag-landing-zone'
SYNC_PRODUCTS_BUCKET = 'synced-products'
PRODUCTS_FILE_PATH = 'data/products.csv'
STREAMS_PREFIX = 'data/'
ARCHIVE_PREFIX = 'archive/'

REQUIRED_COLUMNS = {"id", "title", "description", "link", "image_link", "availability", 
                     "price", "brand", "condition", "product_type"}

# Initialize S3 client with secure access
s3_client = boto3.client('s3', region_name="us-east-1")

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket with the specified prefix """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Files with prefix '{prefix}' from S3: {files}")
        return files
    except Exception as e:
        logging.error(f"Error listing files in S3: {str(e)}")
        raise

def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """ Read a CSV file from S3 """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e:
        logging.error(f"Error reading CSV from S3: {str(e)}")
        raise
    
def validate_datasets():
    validation_results = {}
    try:
        products_files = list_s3_files(STREAMS_PREFIX)
        for products_file in products_files:
            products_data = read_s3_csv(products_file)
            missing_columns = REQUIRED_COLUMNS - set(products_data.columns)
            if not missing_columns:
                validation_results = True
                logging.info(f"All required columns present in {products_file}")
            else:
                validation_results = False
                logging.warning(f"Missing columns in {products_file}: {missing_columns}")
                break
    except Exception as e:
        logging.error(f"Error validating datasets: {str(e)}")
        raise
    return validation_results

def branch_task(ti):
    validation_results = ti.xcom_pull(task_ids='validate_datasets')
    if validation_results:
        return 'data_transformation'
    else:
        logging.error("Validation failed; ending DAG.")
        return 'end_dag'

def data_transformation(ti):
    products_files = list_s3_files(STREAMS_PREFIX)
    products_data = pd.concat([read_s3_csv(file) for file in products_files], ignore_index=True)
    
    transformed_data = [
        {
            "method": "UPDATE",
            "data": {
                "id": str(item["id"]),
                "title": item["title"],
                "description": item["description"],
                "availability": item["availability"],
                "price": item["price"],
                "link": item["link"],
                "image_link": item["image_link"],
                "brand": item["brand"],
                "condition": item["condition"],
                "google_product_category": item["product_type"]
            }
        }
        for item in products_data.to_dict(orient="records")
    ]
    logging.info("Data transformed successfully.")
    ti.xcom_push(key='transformed_products_data', value=transformed_data)

def send_to_facebook_catalog(**kwargs):
    batches = kwargs['ti'].xcom_pull(key='transformed_products_data', task_ids='data_transformation')
    url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/items_batch?item_type=PRODUCT_ITEM"
    headers = {
        "Authorization": f"Bearer {FB_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, headers=headers, json={"requests": batches})
        if response.status_code == 200:
            handles = response.json().get('handles', [])
            kwargs['ti'].xcom_push(key='batch_handles', value=handles)
            logging.info("Data successfully sent to Facebook.")
        else:
            logging.error(f"Error sending batch: {response.text}")
            raise Exception(f"Batch error: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error: {e}")
        raise

def check_batch_status(**kwargs):
    handles = kwargs['ti'].xcom_pull(key='batch_handles', task_ids='send_to_facebook_catalog')
    for handle in handles:
        url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/check_batch_request_status?handle={handle}"
        headers = {"Authorization": f"Bearer {FB_ACCESS_TOKEN}"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                batch_data = response.json().get("data", [{}])[0]
                if batch_data["status"] == "finished":
                    logging.info(f"Batch {handle} completed.")
                else:
                    logging.warning(f"Batch {handle} status: {batch_data['status']}")
            else:
                logging.error(f"Status check error: {response.text}")
                raise Exception("Failed status check.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Status check network error: {e}")
            raise

def move_processed_files():
    """ Move processed files from data to archive in S3 """
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        now = datetime.utcnow().strftime("%Y/%m/%d")
        for file in stream_files:
            destination_key = f"{ARCHIVE_PREFIX}{now}/{file.split('/')[-1]}"
            s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': file}, Key=destination_key)
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=file)
            logging.info(f"Moved {file} to archive.")
    except Exception as e:
        logging.error(f"Error archiving files: {str(e)}")
        raise

def save_product_to_synced_products_bucket(ti):
    transformed_data = ti.xcom_pull(task_ids='data_transformation', key='transformed_products_data')
    try:
        now = datetime.utcnow().strftime("%Y/%m/%d")
        s3_path = f"{now}/transformed_products.json"
        s3_client.put_object(Bucket=SYNC_PRODUCTS_BUCKET, Key=s3_path, Body=json.dumps(transformed_data), ContentType='application/json')
        logging.info(f"Saved transformed data to {SYNC_PRODUCTS_BUCKET}/{s3_path}")
    except Exception as e:
        logging.error(f"Error saving data to S3: {str(e)}")
        raise

with DAG('sync_products_with_facebook', default_args=default_args, schedule_interval='@daily') as dag:
    
    validate_datasets_task = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
    )
    data_transformation_task = PythonOperator(
        task_id='data_transformation',
        python_callable=data_transformation,
    )
    send_to_facebook_task = PythonOperator(
        task_id='send_to_facebook_catalog',
        python_callable=send_to_facebook_catalog,
    )
    check_batch_status_task = PythonOperator(
        task_id='check_batch_status',
        python_callable=check_batch_status,
    )
    move_input_data_task = PythonOperator(
        task_id='move_input_data',
        python_callable=move_processed_files,
    )
    save_transformed_data_task = PythonOperator(
        task_id='save_transformed_data',
        python_callable=save_product_to_synced_products_bucket,
    )
    end_dag_task = DummyOperator(task_id='end_dag')

    validate_datasets_task >> branch_task >> [data_transformation_task, end_dag_task]
    data_transformation_task >> send_to_facebook_task >> check_batch_status_task
    check_batch_status_task >> move_input_data_task >> save_transformed_data_task

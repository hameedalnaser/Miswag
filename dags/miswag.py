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

# Variables defined in Airflow UI
FB_ACCESS_TOKEN = Variable.get("FB_ACCESS_TOKEN")
FACEBOOK_BASE_API_URL = Variable.get("FACEBOOK_BASE_API_URL")
FB_CATALOG_ID = Variable.get("FB_CATALOG_ID")

# Set up logging for monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'dag-landing-zone'
SYNC_PRODUCTS_BUCKET = 'synced-products'
PRODUCTS_FILE_PATH = 'data/products.csv'
STREAMS_PREFIX = 'data/'
ARCHIVE_PREFIX = 'archive/'

# Define a timezone offset of +3 hours
offset = timedelta(hours=3)
now_with_offset = datetime.now() + offset
timestamp = now_with_offset.strftime("%Y-%m-%d/%H-%M-%S")
year = now_with_offset.strftime("%Y")
month = now_with_offset.strftime("%m")
day = now_with_offset.strftime("%d")

REQUIRED_COLUMNS = {"id", "title", "description", "link", "image_link", "availability", 
                     "price", "brand", "condition", "product_type"}

# Initialize S3 client with secure access
s3_client = boto3.client('s3')

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket that match the prefix """

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}, number of files are : {len(files)}")
        return files
    except Exception as e:
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """ Read a CSV file from S3 """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")
    
def data_checking():
    validation_results = {}
    try:
        products_files = list_s3_files(STREAMS_PREFIX)
        for products_file in products_files:
            products_data = read_s3_csv(products_file)
            missing_columns = set(REQUIRED_COLUMNS) - set(products_data.columns)
            if not missing_columns:
                validation_results = True
                logging.info(f"All required columns present in {products_file}")
            else:
                validation_results = False
                logging.warning(f"Missing columns in {products_file}: {missing_columns}")
                break
    except Exception as e:
            validation_results = False
            logging.error(f"Failed to read or validate data from S3: {e}")
            raise
    return validation_results


def data_validation(ti):
    validation_results = ti.xcom_pull(task_ids='data_checking')
    if not isinstance(validation_results, list):
        validation_results = [validation_results]
    # If all validation checks pass
    if all(validation_results):
        return 'data_transformation'
    else:
        logging.error("Validation failed; ending DAG.")
        return 'validation_failed'


def data_transformation(ti):
    products_files = list_s3_files(STREAMS_PREFIX)
    if len(products_files) > 1:
        # Read and merge files.
        dfs = [read_s3_csv(file) for file in products_files]
        products_data = pd.concat(dfs, ignore_index=True)
        products_data.columns = dfs[0].columns
    else:
        products_data = read_s3_csv(products_files[0])

    products_data = products_data.to_dict(orient="records")
    logging.info(f"Number of input products are : {len(products_data)}")
    logging.info("Loaded products data:")
    # Transform data into JSON format for Facebook batch API
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
        for item in products_data
    ]
    ti.xcom_push(key='transformed_products_data', value=transformed_data)
    logging.info("Data transformed successfully.")
    return 'send_to_facebook_catalog'

def send_to_facebook_catalog(**kwargs):
    batches = kwargs['ti'].xcom_pull(key='transformed_products_data', task_ids='data_transformation')
    handles = []
    url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/items_batch?item_type=PRODUCT_ITEM"
    headers = {
        "Authorization": f"Bearer {FB_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, headers=headers, json={"requests": batches})
        response_data = response.json()

        if response.status_code == 200:
            handles = response.json().get('handles', [])
            kwargs['ti'].xcom_push(key='batch_handles', value=handles)
            logging.info("Data successfully sent to Facebook.")
            # handles.extend(response_data['handles'])  # Use extend to add all handles from the list
        else:
            logging.error(f"Error sending batch: {response.text}")
            raise Exception(f"Batch error: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error: {e}")
        raise

def checking_sent_data_handles(**kwargs):
    handles = kwargs['ti'].xcom_pull(key='batch_handles', task_ids='send_to_facebook_catalog')
    for handle in handles:
        url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/check_batch_request_status?handle={handle}"
        headers = {"Authorization": f"Bearer {FB_ACCESS_TOKEN}"}
        try:
            response = requests.get(url, headers=headers)
        # response_data = response.json()
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

def archive_input_data():
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for file in stream_files:
            copy_source = {'Bucket': BUCKET_NAME, 'Key': file}
            destination_key = file.replace('data/', f"archive/{year}/{month}/{day}/")
            s3_client.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=destination_key)
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=file)
            logging.info(f"Moved {file} to {destination_key}")
    except Exception as e:
        logging.error(f"Failed to move files from {STREAMS_PREFIX} to {ARCHIVE_PREFIX}: {str(e)}")
        raise

def save_sent_data(ti):
    transformed_data = ti.xcom_pull(task_ids='data_transformation', key='transformed_products_data')        
    try:
        json_filename = f"{timestamp}.json"
        s3_path = f"{year}/{month}/{day}/{json_filename}"
        s3_client.put_object(
            Bucket=SYNC_PRODUCTS_BUCKET,
            Key=s3_path,
            Body=json.dumps(transformed_data),
            ContentType='application/json'
        )
        logging.info(f"Successfully saved transformed data to s3://{SYNC_PRODUCTS_BUCKET}/{s3_path}")
    except Exception as e:
        logging.error(f"Failed to save transformed data to S3: {str(e)}")
        raise



with DAG('sync_products_with_facebook', default_args=default_args, schedule_interval='@hourly') as dag:
    
    
    data_checking = PythonOperator(
        task_id='data_checking',
        python_callable=data_checking
    )

    data_validation = BranchPythonOperator(
        task_id='data_validation',
        python_callable=data_validation,
        provide_context=True
    )

    data_transformation = BranchPythonOperator(
        task_id='data_transformation',
        python_callable=data_transformation,
        provide_context=True
    )
    validation_failed = DummyOperator(
        task_id='validation_failed'
    )

    send_to_facebook_catalog = PythonOperator(
        task_id='send_to_facebook_catalog',
        python_callable=send_to_facebook_catalog,
        provide_context=True,
        sla=timedelta(hours=1),
    )

    checking_sent_data_handles = PythonOperator(
        task_id='checking_sent_data_handles',
        python_callable=checking_sent_data_handles,
        provide_context=True,
        sla=timedelta(hours=1),
    )
    
    archive_input_data = PythonOperator(
        task_id='archive_input_data',
        python_callable=archive_input_data,
        provide_context=True,
    )
    save_sent_data = PythonOperator(
        task_id='save_sent_data',
        python_callable=save_sent_data,
        provide_context=True,
    )


    data_checking>>data_validation
    data_validation>>data_transformation>>send_to_facebook_catalog >> checking_sent_data_handles>>archive_input_data>>save_sent_data
    data_validation>>validation_failed

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
import requests

# Variables defined in Airflow UI
FB_ACCESS_TOKEN = Variable.get("FB_ACCESS_TOKEN")
FACEBOOK_BASE_API_URL = Variable.get("FACEBOOK_BASE_API_URL")
FB_CATALOG_ID = Variable.get("FB_CATALOG_ID")
# FB_BUSSINES_ID = Variable.get("FB_BUSSINES_ID")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'dag-landing-zone'
PRODUCTS_FILE_PATH = 'data/products.csv'
# USERS_FILE_PATH = 'spotify_data/users.csv'
STREAMS_PREFIX = 'data/'
ARCHIVE_PREFIX = 'archive/'
# AWS_PUBLIC_KEY = Variable.get("AWS_PUBLIC_KEY")
# AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
# AWS_PUBLIC_KEY=''
# AWS_SECRET_KEY=''
# Expected data structure and types
# EXPECTED_COLUMNS = {
#     "id": int,
#     "title": str,
#     "description": str,
#     "link": str,
#     "image_link": str,
#     "availability": str,
#     "price": str,  # Assuming price can be a string (e.g., "25.99 USD")
#     "brand": str,
#     "condition": str,
#     "product_type": str
# }
REQUIRED_COLUMNS = {"id", "title", "description", "link", "image_link", "availability", 
                     "price", "brand", "condition", "product_type"}

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket that match the prefix """
    # session = boto3.Session(
    #     aws_access_key_id=AWS_PUBLIC_KEY,
    #     aws_secret_access_key=AWS_SECRET_KEY,
    #     region_name='eu-central-1'
    # )
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}")
        return files
    except Exception as e:
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """ Helper function to read a CSV file from S3 """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")
    
def validate_datasets():
    validation_results = {}

    # Validate products dataset
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
    logging.info(f"loooooool {products_data}")
    logging.info(f"xxxxxxxxx {products_files}")
    return validation_results


def branch_task(ti):
    # Pull validation results from XCom
    validation_results = ti.xcom_pull(task_ids='validate_datasets')
    logging.info(f"Validation results: {validation_results}")

    # Check if validation_results is already a list, if not, wrap it in a list
    if not isinstance(validation_results, list):
        validation_results = [validation_results]

    # If all validation checks pass
    if all(validation_results):
        # # Retrieve file paths from XCom or another task that lists the files
        # file_paths = ti.xcom_pull(task_ids='list_files_task')  # Assuming 'list_files_task' provides the file paths
        # if file_paths and len(file_paths) > 1:  # Check if there is more than one file
        #     # Read and merge the files, keeping only the header of the first file
        #     dfs = [pd.read_csv(file) for file in file_paths]
        #     merged_df = pd.concat(dfs, ignore_index=True)
        #     merged_df.columns = dfs[0].columns  # Keep header from the first file

        #     # Push merged DataFrame to XCom for further use in the DAG
        #     ti.xcom_push(key='merged_file', value=merged_df.to_dict('records'))  # Convert DataFrame to a list of dicts
        #     logging.info("Files merged successfully in memory.")

        return 'data_transformation'
    # else:
    #     return 'end_dag'

# def data_transformation():
#      # Get the list of product files from S3
#     products_files = list_s3_files(STREAMS_PREFIX)
    
#     # Check if there is more than one file
#     if len(products_files) > 1:
#         # Read and merge the files, keeping only the header of the first file
#         dfs = [read_s3_csv(file) for file in products_files]
#         products_data = pd.concat(dfs, ignore_index=True)
#         products_data.columns = dfs[0].columns  # Use the header from the first file
#     else:
#         # If only one file, read it directly
#         products_data = read_s3_csv(products_files[0])
#     logging.info(products_data)
#     # Return or process products_data as needed, keeping it consistently named
#     return products_data

def data_transformation(ti):
    # Step 1: Load the product files from S3
    products_files = list_s3_files(STREAMS_PREFIX)
    
    # Check if there is more than one file and merge if needed
    if len(products_files) > 1:
        # Read and merge files, keeping only the header from the first file
        dfs = [read_s3_csv(file) for file in products_files]
        products_data = pd.concat(dfs, ignore_index=True)
        products_data.columns = dfs[0].columns  # Keep header from the first file
    else:
        # If only one file, read it directly
        products_data = read_s3_csv(products_files[0])

    # products_data = products_data.to_dict(orient="records")
    products_data = json.loads(products_data)
    logging.info("Loaded products data:")
    logging.info(products_data)

    # Step 2: Transform data into JSON format for Facebook batch API
    transformed_data = [
        {
            "method": "UPDATE",
            "data": {
                "id": item["id"],
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
    logging.info(transformed_data)
    # Step 3: Push transformed data to XCom for the next task
    ti.xcom_push(key='transformed_products_data', value=transformed_data)
    logging.info("Transformed data pushed to XCom.")
    return 'send_to_facebook_catalog'

def send_to_facebook_catalog(**kwargs):
    """Send each batch to Facebook's catalog API."""
    batches = kwargs['ti'].xcom_pull(key='transformed_batches', task_ids='transform_data')
    handles = []
    logging.info(batches)
    # for batch in batches:
    url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/items_batch?item_type=PRODUCT_ITEM"
    headers = {
        "Authorization": f"Bearer {FB_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json={"requests": batches})
    response_data = response.json()

    if response.status_code == 200 and 'handles' in response_data:
        handles.extend(response_data['handles'])  # Use extend to add all handles from the list
    else:
        raise Exception(f"Failed to send batch: {response.status_code} - {response.text}")

    kwargs['ti'].xcom_push(key='batch_handles', value=handles)

def check_batch_status(**kwargs):
    """Check the status of each batch in Facebook's API."""
    handles = kwargs['ti'].xcom_pull(key='batch_handles', task_ids='send_to_facebook_catalog')
    
    for handle in handles:
        url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/check_batch_request_status?handle={handle}"
        headers = {"Authorization": f"Bearer {FB_ACCESS_TOKEN}"}
        response = requests.get(url, headers=headers)
        response_data = response.json()
        
        if response.status_code == 200 and "data" in response_data:
            batch_data = response_data["data"][0]
            if batch_data["status"] == "finished":
                errors = batch_data.get("errors", [])
                warnings = batch_data.get("warnings", [])
                
                if errors:
                    print("Errors found:", errors)
                if warnings:
                    print("Warnings found:", warnings)
                
                if not errors:
                    print("Batch processed successfully with no errors.")
            else:
                print(f"Batch process status: {batch_data['status']}")
        else:
            raise Exception(f"Failed to check batch status: {response.status_code} - {response.text}")


def move_processed_files():
    s3 = boto3.client('s3')
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for file in stream_files:
            copy_source = {'Bucket': BUCKET_NAME, 'Key': file}
            destination_key = file.replace('data/', 'archive/')
            s3.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=destination_key)
            s3.delete_object(Bucket=BUCKET_NAME, Key=file)
            logging.info(f"Moved {file} to {destination_key}")
    except Exception as e:
        logging.error(f"Failed to move files from {STREAMS_PREFIX} to {ARCHIVE_PREFIX}: {str(e)}")
        raise
def save_product_to_synced_products_bucket(ti):
    s3 = boto3.client('s3')
    transformed_data = ti.xcom_pull(task_ids='data_transformation_task', key='transformed_products_data')

    if transformed_data:
        # Create the S3 path based on the current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d/%H-%M-%S")
        json_filename = f"{timestamp}.json"
        s3_path = f"month/day/{json_filename}"  # Update this to your actual path
        
        # Save transformed data to S3 as JSON
        try:
            s3.put_object(
                Bucket='your-bucket-name',  # Replace with your S3 bucket name
                Key=s3_path,
                Body=json.dumps(transformed_data),
                ContentType='application/json'
            )
            logging.info(f"Successfully saved transformed data to s3://synced-products/{s3_path}")
        except Exception as e:
            logging.error(f"Failed to save transformed data to S3: {str(e)}")
            raise
    else:
        logging.warning("No transformed data to save.")



with DAG('sync_products_with_facebook', default_args=default_args, schedule_interval='@daily') as dag:
    
    
    validate_datasets = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task,
        provide_context=True
    )

    data_transformation = BranchPythonOperator(
        task_id='data_transformation',
        python_callable=data_transformation,
        provide_context=True
    )
    end_dag = DummyOperator(
        task_id='end_dag'
    )

    send_to_facebook_task = PythonOperator(
        task_id='send_to_facebook_catalog',
        python_callable=send_to_facebook_catalog,
        provide_context=True,
        # on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )

    check_batch_status_task = PythonOperator(
        task_id='check_batch_status',
        python_callable=check_batch_status,
        provide_context=True,
        # on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )
    
    move_input_data = PythonOperator(
        task_id='move_input_data',
        python_callable=move_processed_files,
        provide_context=True,
    )
    save_transformed_data_task = PythonOperator(
        task_id='save_transformed_data_task',
        python_callable=save_product_to_synced_products_bucket,
        provide_context=True,
    )


    validate_datasets>>check_validation>>data_transformation>>send_to_facebook_task >> check_batch_status_task>>move_input_data>>save_transformed_data_task

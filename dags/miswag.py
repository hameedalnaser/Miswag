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
EXPECTED_COLUMNS = {
    "id": int,
    "title": str,
    "description": str,
    "link": str,
    "image_link": str,
    "availability": str,
    "price": str,  # Assuming price can be a string (e.g., "25.99 USD")
    "brand": str,
    "condition": str,
    "product_type": str
}
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
            logging.error(f"Failed to read or validate streams from S3: {e}")
            raise

    return validation_results

def branch_task(ti):
    validation_results = ti.xcom_pull(task_ids='validate_datasets')
    
    if all(validation_results.values()):
        return 'calculate_genre_level_kpis'
    else:
        return 'end_dag'








# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import pandas as pd
# import logging

# REQUIRED_COLUMNS = {"id", "title", "description", "link", "image_link", "availability", 
#                     "price", "brand", "condition", "product_type"}

# BUCKET_NAME = "your_bucket_name"
# STREAMS_PREFIX = "your_prefix"

# def list_s3_files(prefix, bucket=BUCKET_NAME):
#     """List all files in S3 bucket that match the prefix using Airflow's S3Hook."""
#     s3_hook = S3Hook(aws_conn_id="aws_default")
#     try:
#         files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
#         csv_files = [file for file in files if file.endswith('.csv')]
#         logging.info(f"Successfully listed files with prefix {prefix} from S3: {csv_files}")
#         return csv_files
#     except Exception as e:
#         logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
#         raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

# def read_s3_csv(file_name, bucket=BUCKET_NAME):
#     """Helper function to read a CSV file from S3 using S3Hook."""
#     s3_hook = S3Hook(aws_conn_id="aws_default")
#     try:
#         obj = s3_hook.get_key(key=file_name, bucket_name=bucket)
#         logging.info(f"Successfully read {file_name} from S3")
#         return pd.read_csv(obj.get()['Body'])
#     except Exception as e:
#         logging.error(f"Failed to read {file_name} from S3: {str(e)}")
#         raise Exception(f"Failed to read {file_name} from S3: {str(e)}")

# def validate_datasets():
#     """Validate if all required columns are present in S3 CSV files."""
#     validation_results = {}

#     try:
#         products_files = list_s3_files(STREAMS_PREFIX)
#         for products_file in products_files:
#             products_data = read_s3_csv(products_file)
#             missing_columns = REQUIRED_COLUMNS - set(products_data.columns)
#             if not missing_columns:
#                 validation_results[products_file] = True
#                 logging.info(f"All required columns present in {products_file}")
#             else:
#                 validation_results[products_file] = False
#                 logging.warning(f"Missing columns in {products_file}: {missing_columns}")
#                 break
#     except Exception as e:
#         logging.error(f"Failed to read or validate datasets from S3: {e}")
#         raise

#     return validation_results

# def branch_task(ti):
#     """Branch to different tasks based on validation results."""
#     validation_results = ti.xcom_pull(task_ids='validate_datasets')
    
#     if all(validation_results.values()):
#         return 'calculate_genre_level_kpis'
#     else:
#         return 'end_dag'








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


    validate_datasets>>check_validation

# DAG file for BACKFEED UTILITY
# 
import boto3
import json
import yaml
import botocore.session

from datetime import date, datetime, time, timedelta

# Airflow packages import
from airflow import DAG, utils
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


# Create bucket and key by splitting filename
def path_to_bucket_key(path):
    if not path.startswith('s3://'):
        raise ValueError('S3 path provided is incorrect: ' + path)
    path = path[5:].split('/')
    bucket = path[0]
    key = '/'.join(path[1:])
    return bucket, key


# Read the S3 bucket file and reeturn data as string
def read_s3_file(filepath, encoding='utf8'):
    client = boto3.client('s3')
    bucket, key = path_to_bucket_key(filepath)
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode(encoding) 


# Extract the AWS account id
with open('/usr/local/airflow/ssh/variables.json') as json_file:
    data = json.load(json_file)
aws_account_id = data['AccountId']

KEY_FILE = "ec2/pem_file_path/ec2_key.pem"
# IP for machine where job needs to be submitted. In this case EMR Master node IP.
IP = "192.164.1.1"
queue = "sqs-airfow-queue"
script_path = "/home/user/utilities/backfeed_generator"

basel_backfeed_config = f"s3://bucket-{aws_account_id}-code/dags/config/backfeed_config.yaml"
BACKFEED_CONFIG_FILE = read_s3_file(basel_backfeed_config)
BACKFEED_CONTENT = BACKFEED_CONFIG_FILE.format(aws_acct_id=aws_account_id)
BACKFEED_DICT = yaml.safe_load(BACKFEED_CONTENT)
feed_name = "exposure_feed"
FEED_CONFIG = BACKFEED_DICT.get(feed_name)

if FEED_CONFIG.get("sqls"):
    SQLS_DICT = FEED_CONFIG.get("sqls") 
else:
    SQLS_DICT = {"FEED_TBL": FEED_CONFIG.get("table_name")}

# sshing into emr
ssh_cmd = "ssh -o StrictHostKeyChecking=no -t -i {KEY} hadoop@{IP} ".format(KEY=KEY_FILE, IP=IP)

# Defining the default arguments of DAG
default_args = {
    'owner': 'frosty',
    'depends_on_past': False,
    'email': ['geekfrosty@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': queue,
    'retries': 0,
    'end_date': datetime(2099, 12, 31)
}

with DAG(
    dag_id="backfeed_generator",
    start_date=datetime(2023, 11, 3),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    prep_env = BashOperator(task_id='prepare_environment',
                        bash_command=f"{ssh_cmd} sudo python3 {script_path}/prep_env.py --feed_name {feed_name}",
                        do_xcom_push=True)
    end = EmptyOperator(task_id='end')

    start >> prep_env

    for sname in SQLS_DICT.keys():
        table_to_s3 = BashOperator(task_id=f'{sname}_table_to_s3',
                        bash_command=f"""{ssh_cmd} spark-submit -v --master=yarn --driver-memory=19g \
                                            --conf spark.driver.cores=5 \
                                            --conf spark.driver.memoryOverhead=2g \
                                            --conf spark.dynamicAllocation.maxExecutors=16 \
                                            --conf spark.default.parallelism=200  --conf spark.sql.shuffle.partitions=200 \
                                            --conf spark.dynamicAllocation.executorIdleTimeout=300 \
                                            --conf spark.sql.broadcastTimeout=3600 {script_path}/table_to_s3.py --sql_name {sname} {{{{ti.xcom_pull(task_ids='prepare_environment')}}}}""")
        
        parts_to_emr = BashOperator(task_id=f'{sname}_parts_to_emr',
                        bash_command=f"{ssh_cmd} sudo python3 {script_path}/parts_from_s3.py --sql_name {sname} {{{{ti.xcom_pull(task_ids='prepare_environment')}}}}",
                        do_xcom_push=True)
        
        basel_backfeed = BashOperator(task_id=f'{sname}_generate_backfeed',
                        bash_command=f"{ssh_cmd} sudo python3 {script_path}/generate_backfeed.py {{{{ti.xcom_pull(task_ids='{sname}_parts_to_emr')}}}}",
                        do_xcom_push=True)

        emr_to_s3 = BashOperator(task_id=f'{sname}_emr_to_s3',
                        bash_command=f"{ssh_cmd} sudo python3 {script_path}/copy_to_s3.py {{{{ti.xcom_pull(task_ids='{sname}_generate_backfeed')}}}}",
                        do_xcom_push=True)
        prep_env >> table_to_s3 >> parts_to_emr >> basel_backfeed >> emr_to_s3 >> end
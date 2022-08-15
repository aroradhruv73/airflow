## ma_ingest_TVSquared_Action_Files_AWS

import zipfile
#from io import BytesIO
#from datetime import * 
from io import BytesIO 
#import json 
#import re
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta
#from airflow import utils
#from airflow.operators.hive_operator import HiveOperator
#import pysftp
#import logging
#import os, fnmatch
#from operator import itemgetter
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
#import logging
from urllib.parse import urlparse
'''
from tempfile import NamedTemporaryFile
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
'''
from airflow.contrib.hooks.aws_hook import AwsHook

from lib.emr_launch_lib_v1 import AddEMRStepOperator, EMRStepSensor, CreateEMROperator, CreateEMRSensor, TerminateEMROperator

from lib.edt_utils import EDTAlerts, evaluate_dag_run
from airflow.utils.trigger_rule import TriggerRule

from airflow.contrib.operators.s3_to_gcs_operator import S3ToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from lib.edt_utils import CreateLocalSuccessFileOperator

env = Variable.get('aws_environment')
BOOTSTRAP_SCRIPT = f"s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/shell-script/bootstrap.sh"
cluster_name = f'tvsquare_action_' + "{{ ds_nodash }}"
emr_setup = {"num_core_nodes": "2", "master_instance_type": "m5.4xlarge", "core_instance_type": "r5.4xlarge"}

script_loc = '/home/025d3777f4b1ecm/automations/marketing_analytics/marketing_channel_tv/'

source_aws_conn_id = f"dmz_{env}_conn"  #'dmz_prod_conn'
dest_aws_conn_id = f"ckp-etl-{env}"

source_s3_key = f'gd-dataexchange-{env}-tvsquare.s3.us-west-2.amazonaws.com/tvsquare_input_files' #'gd-dataexchange-prod-tvsquare.s3.us-west-2.amazonaws.com/actiondata'
dest_s3_key = f'gd-ckpetlbatch-{env}-analytic/advertising_analytic/tv_square_action/'

dmz = BaseHook.get_connection(source_aws_conn_id)
SOURCE_AWS_ACCESS_KEY_ID = dmz.login
SOURCE_AWS_SECRET_ACCESS_KEY = dmz.password

ckp = BaseHook.get_connection(dest_aws_conn_id)
DESTINATION_AWS_ACCESS_KEY_ID = ckp.login
DESTINATION_AWS_SECRET_ACCESS_KEY = ckp.password

spark_cmd_tvaction = f"spark-submit s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/data_ingestion/tvsquareactionpostlog/tv_square_action_parser.py -en {env}" 
            
spark_cmd_tvaction_file_unzipping = f"spark-submit s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/data_ingestion/tvsquareactionpostlog/tv_square_action_file_unzipping.py -en {env}"
spark_cmd_tvsq_action_for_withoutpartition = f"""spark-submit s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/data_ingestion/tvsquareactionpostlog/tv_square_action_laketable.py -en {env}"""

conn_source = AwsHook(aws_conn_id=source_aws_conn_id)
s3_source_client = conn_source.get_client_type('s3', region_name='us-west-2')
s3_source_resource = conn_source.get_resource_type('s3', region_name='us-west-2')

conn_dest = AwsHook(aws_conn_id=dest_aws_conn_id)
s3_dest_client = conn_dest.get_client_type('s3', region_name='us-west-2')
s3_dest_resource = conn_dest.get_resource_type('s3', region_name='us-west-2')

## Bucket and key (containing tv_square ott data for inserting data into big query table)
s3_bucket_name = f'gd-ckpetlbatch-{env}-analytic'
s3_file_path = 'advertising_analytic_staging/tvsq_action_mart_unpartitioned/'

bq_env = Variable.get('gcs_environment') 
gcs_conn_id = 'bigquery_default'
gcs_bucket_name = 'advertising-enablement-data-transfer'
gcs_file_path = f"gs://{gcs_bucket_name}/{bq_env}_advertising_channels/tvsq_action_mart_aws/"
stg_table_name = 'bq_imports.tvsq_action_mart_aws'
bq_conn_id = 'bigquery_default'
today = datetime.today()

filepath_ingcs = f'{bq_env}_advertising_channels/tvsq_action_mart_aws/advertising_analytic_staging/tvsq_action_mart_unpartitioned/*.parquet'

def s3tos3transfer(**kwargs):
        dir = 'tvsquare_input_files/tvsq_tvactiondata_'
        s3_bucket = f'gd-dataexchange-{env}-tvsquare' #'gd-dataexchange-prod-tvsquare'
        s3_bucket_boto3 = s3_source_resource.Bucket(f'gd-dataexchange-{env}-tvsquare')
        
        response = s3_source_client.list_objects_v2(Bucket=f'gd-dataexchange-{env}-tvsquare', Prefix=dir)
        
        if response['KeyCount'] > 0:
            all = response['Contents']
            
            for file in all:
                source_response = s3_source_client.get_object(
                  Bucket=f'gd-dataexchange-{env}-tvsquare',
                  Key=file['Key']
                )

                s3_dest_resource.meta.client.upload_fileobj(
                  source_response['Body'],
                  f'gd-ckpetlbatch-{env}-analytic',
                  'advertising_analytic_staging/tv_square_action_zippedfiles/' + file['Key'].split('/')[-1],
                )
            
        #if len(response['Contents']) - 1 >= 1:
        if response['KeyCount'] > 0:
           return 'tsk_create_emr'
        else:
           return 'branch_no_tasks'
        
def unzipping_files(**kwargs):
    conn_source = AwsHook(aws_conn_id=dest_aws_conn_id)
    
    s3_client = conn_source.get_client_type('s3', region_name='us-west-2')
    s3 = conn_source.get_resource_type('s3', region_name='us-west-2')
    s3_bucket = f'gd-ckpetlbatch-{env}-analytic'
    s3_bucket_boto3 = s3.Bucket(f'gd-ckpetlbatch-{env}-analytic')
    
    prefix = 'advertising_analytic/tv_square_action_zippedfiles/tvsq_tvactiondata_'
    suffix = 'zip'
    dir = 'advertising_analytic/tv_square_action_zippedfiles'
    S3_UNZIPPED_FOLDER = 'advertising_analytic/tv_square_action_unzippedfiles/'

    files_in_s3 = [f.key for f in s3_bucket_boto3.objects.filter(Prefix=dir).all()]	

    files_tvs_zip = []
    for i in files_in_s3:
        if i.startswith(prefix) and i.endswith(suffix):
           files_tvs_zip.append(i)
    
    #for i in files_tvs_zip:
    #    print(i)   

    for obj in files_tvs_zip:
        zip_obj = s3.Object(bucket_name=s3_bucket,key=obj)
        buffer = BytesIO(zip_obj.get()["Body"].read())
        z = zipfile.ZipFile(buffer)
        for filename in z.namelist(): 
            file_info = z.getinfo(filename)   
            #print(f"Copying file {filename} to {S3_BUCKET}/{S3_UNZIPPED_FOLDER}{filename}") 
            response = s3_client.put_object( 
            Body=z.open(filename).read(),Bucket=s3_bucket,Key=f'{S3_UNZIPPED_FOLDER}{filename}' 
            )
            
def archivefiles(**kwargs):
    s3_bucket = s3_dest_resource.Bucket(f'gd-ckpetlbatch-{env}-analytic')
    dir = 'advertising_analytic_staging/tv_square_action_zippedfiles'
    files_in_s3 = [f.key for f in s3_bucket.objects.filter(Prefix=dir).all()]

    prefix = 'advertising_analytic_staging/tv_square_action_zippedfiles/tvsq_tvactiondata_'
    suffix = 'zip'
    source_bucket = f'gd-ckpetlbatch-{env}-analytic'

    files_tvs_zip = []
    for i in files_in_s3:
        if i.startswith(prefix) and i.endswith(suffix):
           files_tvs_zip.append(i)

    for i in files_tvs_zip:
        print(i)

    for i in files_tvs_zip:
        print(i)
        ## Archiving processed file
        copy_source = {
        'Bucket': source_bucket,
        'Key': i
        }
        s3_bucket.meta.client.copy(copy_source,f'gd-ckpetlbatch-{env}-analytic', 'advertising_analytic_staging/tv_square_action_archive/' + i)
        
        ## Deleting processed file from source (processing) folder
        response = s3_dest_client.delete_object(
        Bucket = source_bucket,
        Key=i
        )
        
        getfilename = i.split('/')[2]
        j = 'tvsquare_input_files/' + getfilename
        
        ## Deleting processed files from DMZ account (gd-dataexchange-prod-tvsquare/actiondata folder)
        response1 = s3_source_client.delete_object(
        Bucket = f'gd-dataexchange-{env}-tvsquare',
        Key=j
        )           
       
        
default_args = {
    'owner': 'ACDC',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 10),
    'email': ['darora@godaddy.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag_name = 'ma_ingest_TVSquared_Action_Files_AWS'

dag = DAG(dag_id=dag_name,
          default_args=default_args,
          template_searchpath=script_loc,
          schedule_interval='0 5,17 * * *')

start_tv = DummyOperator(
    task_id='start_batch_tv',
    dag=dag
    )

branch_no_tasks = DummyOperator(
    task_id='branch_no_tasks',
    dag=dag
)
    
s3tos3_task = BranchPythonOperator(
    task_id='s3tos3',
    python_callable=s3tos3transfer,
    do_xcom_push=False,
    dag=dag)

tsk_create_emr = CreateEMROperator(
                task_id='tsk_create_emr',
                aws_conn_id=dest_aws_conn_id,
                emr_cluster_name=cluster_name,
                num_core_nodes=emr_setup["num_core_nodes"],
                master_instance_type=emr_setup["master_instance_type"],
                core_instance_type=emr_setup["core_instance_type"],
                emr_sc_bootstrap_file_path=BOOTSTRAP_SCRIPT,
                data_pipeline_name=dag_name,
                dag=dag,
)

emr_sensor = CreateEMRSensor(
    task_id='tsk_emr_sensor',
    aws_conn_id=dest_aws_conn_id,
    provisioned_record_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr', key='return_value')[0] }}",
    retries=1,
    dag=dag,
)


tsk_emr_step_tvsqaction_file_unzipping = AddEMRStepOperator(
    task_id='tv_square_action_file_unzipping',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_cmd=spark_cmd_tvaction_file_unzipping,
    step_name='tv_square_action_file_unzipping',
    dag=dag,
)
tsk_emr_step_tvsqaction_file_unzipping_sensor = EMRStepSensor(
    task_id='emr_step_sensor_tv_square_action_file_unzipping',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='tv_square_action_file_unzipping', key='return_value')[0] }}",
    dag=dag,
)

tsk_emr_step_tvsqaction = AddEMRStepOperator(
    task_id='tv_square_action',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_cmd=spark_cmd_tvaction,
    step_name='tv_square_action',
    dag=dag,
)
tsk_emr_step_tvsqaction_sensor = EMRStepSensor(
    task_id='emr_step_sensor_tv_square_action',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='tv_square_action', key='return_value')[0] }}",
    dag=dag,
)

tv_square_action_withoutpartitionedCols = AddEMRStepOperator(
    task_id='tv_square_action_withoutpartitionedCols',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_cmd=spark_cmd_tvsq_action_for_withoutpartition,
    step_name='tv_square_action_withoutpartitionedCols',
    dag=dag,
)
tv_square_action_withoutpartitionedCols_sensor = EMRStepSensor(
    task_id='emr_step_sensor_tv_square_action_withoutpartitionedCols_sensor',
    aws_conn_id=dest_aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_emr_sensor', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='tv_square_action_withoutpartitionedCols', key='return_value')[0] }}",
    dag=dag,
)

tsk_terminate_emr = TerminateEMROperator(
    task_id='terminate_emr',
    aws_conn_id=dest_aws_conn_id,
    provisioned_product_id="{{task_instance.xcom_pull(task_ids='tsk_create_emr', key='return_value')[1] }}",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

archivefiles_task = PythonOperator(
    task_id='archivefiles',
    python_callable=archivefiles,
    dag=dag)

end_tv = DummyOperator(
    task_id='end_batch_tv',
    dag=dag
    )
    
tsk_s3_to_gcs = S3ToGoogleCloudStorageOperator(
    task_id='tsk_s3_to_gcs',
    aws_conn_id=dest_aws_conn_id,
    bucket=s3_bucket_name,
    prefix=s3_file_path,
    dest_gcs_conn_id=gcs_conn_id,
    dest_gcs=gcs_file_path,
    dag=dag
)

tsk_load_data_in_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='tsk_load_data_in_bq',
    bucket=gcs_bucket_name,
    source_objects=[filepath_ingcs],
    destination_project_dataset_table=stg_table_name,
    source_format='PARQUET',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    google_cloud_storage_conn_id=gcs_conn_id,
    bigquery_conn_id=bq_conn_id,
    autodetect=True,
    dag=dag
)

delete_gcs_old_files = GoogleCloudStorageDeleteOperator(
    task_id='delete_gcs_old_files',
    bucket_name=urlparse(gcs_file_path).netloc,
    prefix=urlparse(gcs_file_path).path.lstrip('/'),
    google_cloud_storage_conn_id=gcs_conn_id,
    dag=dag
)

tsk_load_data_in_bq_main = BigQueryOperator(
    task_id='tsk_load_data_in_bq_main',
    use_legacy_sql=False,
    bql='tv_square_action_stgtomain_aws_bq.sql',
    dag=dag)
	
publish_success_file = CreateLocalSuccessFileOperator(
    task_id='publish_success_file',
    db_name="cue",
    table_name="mrd_test_table",
    aws_environment=env,
    aws_conn_id=dest_aws_conn_id,
    success_file_year=f"{today.year}",
    success_file_month=today.strftime('%m'),
    success_file_day=today.strftime('%d'),
    success_file_name='_SUCCESS',
    dag=dag
)		

# Final notification, if any task failed then mark the whole dag as failed and send failure notification else success notification
assess_dagrun_and_log_action = PythonOperator(
    task_id='evaluate_dag_run_tvsq_action_aws',
    provide_context=True,
    python_callable=evaluate_dag_run,
    op_kwargs={
        'send_success_email': True,
        'failure_email_recipients': 'darora@godaddy.com',
        'log_actionlog': False,
        'actionlog_db_name': 'enterprise',
        'aws_conn_id': dest_aws_conn_id,
        'aws_environment': env
        },
    trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    retries=0,
    dag=dag,
)
    
start_tv >> s3tos3_task >> [branch_no_tasks,tsk_create_emr] 
tsk_create_emr >> emr_sensor >> tsk_emr_step_tvsqaction_file_unzipping >> tsk_emr_step_tvsqaction_file_unzipping_sensor >> tsk_emr_step_tvsqaction >> tsk_emr_step_tvsqaction_sensor >> tv_square_action_withoutpartitionedCols >> tv_square_action_withoutpartitionedCols_sensor >> tsk_terminate_emr >> archivefiles_task>> end_tv >> assess_dagrun_and_log_action
tsk_terminate_emr >> delete_gcs_old_files >> tsk_s3_to_gcs >> tsk_load_data_in_bq >> tsk_load_data_in_bq_main >> publish_success_file >> end_tv
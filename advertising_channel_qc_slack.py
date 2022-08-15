import os
import logging
from boto3 import client
import pandas as pd
import pyarrow
import io
from datetime import datetime

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.utils.state import State
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pytz
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.s3_key_sensor import S3KeySensor

from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# from lib.emr_operator import EMRClusterCreateOperator, EMRAddStepOperator, EMRClusterDeleteOperator, EMRStepSensor


from lib.emr_launch_lib_v1 import CreateEMRSensor, CreateEMROperator
from lib.emr_launch_lib_v1 import AddEMRStepOperator, EMRStepSensor
from lib.emr_launch_lib_v1 import TerminateEMROperator

from lib.acdc_alert_v2 import acdc_slack_generic_alert

retry_num=0
send_email=True
email_list = ['darora@godaddy.com']
dag_name='advertising_channel_qc_sunday'
aws_env = "{{ var.value.mwaa_env }}"
aws_conn_id='ckp-etl-dev-private'

default_core_instance_type = 'm5.4xlarge'
schedule_interval='50 12 * * *'

dag_date=datetime.strptime('2021-06-07', "%Y-%m-%d")

default_args = {
    'owner': 'ACDC',
    'start_date': dag_date,
    'email': email_list,
    'email_on_failure': send_email,
    'email_on_retry': False,
    'retries': retry_num,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False
}

dag = DAG(dag_name,
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval=schedule_interval,
    description='advertising_channel_qc_slack',
)

bucket_name=f'gd-ckpetlbatch-dev-private-code'
prefix_string=f'GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/checksjson/'

#read json files of all channels in checksjson folder

ch_ls=[]
for key in boto3.client('s3').list_objects(Bucket=bucket_name, Prefix=prefix_string)['Contents']:
    ch_ls.append(key['Key'])
# ch_ls=ch_ls[1:]

# Provide a unique EMR cluster name per run:
now = datetime.now()

EMR_TAGS = [
    {"Key": "dataPipeline",     "Value": dag_name},
    {"Key": "teamName",         "Value": "ACDC"},
    {"Key": "organization",     "Value": "D-SPA"},
    {"Key": "onCallGroup",      "Value": "Dev-GMODataEngineering"},
    {"Key": "teamSlackChannel", "Value": "acdc_alerts"},
    {"Key": "managedByMWAA",    "Value": "true"},
    {"Key": "doNotShutDown",    "Value": "true"},
]


# aws_env = "{{ var.value.mwaa_env }}"
# aws_conn_id=f'ckp-etl-dev-private'
# cluster_name = f'pydeequairflowcluster-{ datetime.now().strftime("%M%S") }'

cluster_name = f'pydeequairflowcluster-test13'

# table_name='bill_line'
# source_table_name='ads_bill_line'

# Note: This step will likely take ~15mins
provision_emr = CreateEMROperator(
                task_id="provision_emr",
                aws_conn_id=aws_conn_id,
                aws_environment=aws_env,
                emr_cluster_name=cluster_name,
                master_instance_type="m5.xlarge",
                mum_core_nodes=2,
                core_instance_type="m5.xlarge",
                data_pipeline_name='pydeequairflowcluster',
                emr_sc_bootstrap_file_path=f's3://gd-ckpetlbatch-dev-private-code/GDLakeDataProcessors/advertising-enablement/qc-framework/emrbootstrapfolder/bootstrap.sh',
                emr_sc_tags=EMR_TAGS,
                dag=dag,
                )
                
delete_emr = TerminateEMROperator(
                task_id='delete_emr',
                aws_conn_id=aws_conn_id,
                aws_environment=aws_env,
                provisioned_product_id="{{ task_instance.xcom_pull(task_ids='provision_emr', key='return_value')[1] }}",
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )

check_emr = CreateEMRSensor(
    task_id='check_emr',
    aws_conn_id=aws_conn_id,
    aws_environment=aws_env,
    provisioned_record_id="{{ task_instance.xcom_pull(task_ids='provision_emr', key='return_value')[0] }}",
    on_failure_callback=delete_emr,
    dag=dag,
)

                
# def steplist(ch):
        # return  AddEMRStepOperator(
                # task_id='emr_step_'+ch.split('/')[-1].split('.')[0],
                # aws_conn_id=aws_conn_id,
                # aws_environment=aws_env,
                # job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                # step_cmd=f"spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.submit.waitAppCompletion=true --packages com.amazon.deequ:deequ:1.0.3 --exclude-packages net.sourceforge.f2j:arpack_combined_all s3a://gd-ckpetlbatch-dev-private-code/GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/Pdq_test.py %s %s " %(bucket_name,ch),
                # step_name='emr_step_'+ch.split('/')[-1].split('.')[0],
                # dag=dag,
            # )
# on_failure_callback=delete_emr,


emr_step_audio = AddEMRStepOperator(
                task_id='emr_step_audio',
                aws_conn_id=aws_conn_id,
                aws_environment=aws_env,
                job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                step_cmd=f"spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.submit.waitAppCompletion=true --packages com.amazon.deequ:deequ:1.0.3 --exclude-packages net.sourceforge.f2j:arpack_combined_all s3a://gd-ckpetlbatch-dev-private-code/GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/Pdq_test.py  gd-ckpetlbatch-dev-private-code GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/checksjson/audio.json ",
                step_name='emr_step_audio',
                dag=dag,
            )
            
emr_step_video = AddEMRStepOperator(
                task_id='emr_step_video',
                aws_conn_id=aws_conn_id,
                aws_environment=aws_env,
                job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                step_cmd=f"spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.submit.waitAppCompletion=true --packages com.amazon.deequ:deequ:1.0.3 --exclude-packages net.sourceforge.f2j:arpack_combined_all s3a://gd-ckpetlbatch-dev-private-code/GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/Pdq_test.py  gd-ckpetlbatch-dev-private-code GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/checksjson/video.json ",
                step_name='emr_step_video',
                dag=dag,
            )
#on_failure_callback=delete_emr
emr_step_social = AddEMRStepOperator(
                task_id='emr_step_social',
                aws_conn_id=aws_conn_id,
                aws_environment=aws_env,
                job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                step_cmd=f"spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.submit.waitAppCompletion=true --packages com.amazon.deequ:deequ:1.0.3 --exclude-packages net.sourceforge.f2j:arpack_combined_all s3a://gd-ckpetlbatch-dev-private-code/GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/Pdq_test.py  gd-ckpetlbatch-dev-private-code GDLakeDataProcessors/advertising-enablement/qc-framework/inputs/checksjson/social.json ",
                step_name='emr_step_social',
                dag=dag,
            )           

check_emr_social_complete = EMRStepSensor(
                     task_id='emr_steps_social_completion',
					 aws_conn_id=aws_conn_id,
                     aws_environment=aws_env,
                     job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                     step_id="{{ task_instance.xcom_pull(task_ids='emr_step_social', key='return_value')[0] }}",
                     dag=dag,
                 )
# on_failure_callback=delete_emr,

check_emr_audio_complete = EMRStepSensor(
                     task_id='emr_steps_audio_completion',
					 aws_conn_id=aws_conn_id,
                     aws_environment=aws_env,
                     job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
                     step_id="{{ task_instance.xcom_pull(task_ids='emr_step_audio', key='return_value')[0] }}",
                     dag=dag,
                 )

step_id="{{ task_instance.xcom_pull(task_ids='load_dim_chargeback_review', key='return_value')[0] }}"

			   
start_func = DummyOperator(
             task_id='Start',
             dag=dag
             )
        
complete = DummyOperator(
           task_id='All_jobs_completed',
           trigger_rule=TriggerRule.ALL_DONE,
           dag=dag
           )
		   


def pd_read_s3_multiple_parquets(filepath, bucket, s3=None,
                                 s3_client=None, verbose=False, **args):
    if not filepath.endswith('/'):
        filepath = filepath + '/'  # Add '/' to the end

    s3_keys = [item.key for item in s3.Bucket(bucket).objects.filter(Prefix=filepath) if item.key.endswith('.parquet')]
    s3_keys = [key for key in s3_keys if 'checks' in key]

    if not s3_keys:
        print('No parquet found in', bucket, filepath)

    dfs = [pd.read_parquet(io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()), **args)
           for key in s3_keys]
    return pd.concat(dfs, ignore_index=True) if len(dfs)>1 else pd.DataFrame(dfs[0])

bucket_name=f'gd-ckpetlbatch-dev-private-code'


b3s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

check_res = {}

for ch in ch_ls:
   if ch.split('/')[-1].split('.')[0] == 'audio':
      filepath='GDLakeDataProcessors/advertising-enablement/qc-framework/outputs'+'/'+ch.split('/')[-1].split('.')[0]
      df = pd_read_s3_multiple_parquets(filepath, bucket_name, b3s3, s3_client, verbose=False)
      print("df dataframe: ")
     # print(df)
     # print("df type: ", type(df))
     # print("df describe: ", df.info())
     # print("df columns: ", list(df.columns))
      check_res[ch]= df.loc[df['constraint_status']=='Failure']

print(check_res)

                
failure_slack_notification = PythonOperator(
    task_id="failure_slack_notification",
    provide_context=True,
    python_callable=acdc_slack_generic_alert,
    op_args=[ (check_res) ],
    trigger_rule=TriggerRule.ONE_FAILED
)


print("============= Run EMR Step ================")

# emr_step_1 = steplist(ch_ls[0])

# emr_step_0 = steplist(ch_ls[0])

# emr_step_1 = steplist(ch_ls[1])

# emr_step_2 = steplist(ch_ls[2])

# start_func >> provision_emr >> check_emr >> emr_step_0 >> emr_step_1 >> emr_step_2 >> check_emr_steps_complete >> delete_emr >> complete
# start_func >> provision_emr >> check_emr >> emr_step_audio >> emr_step_video >> emr_step_social >> check_emr_steps_complete >> delete_emr >> complete

# start_func >> provision_emr >> check_emr >> emr_step_audio >> check_emr_audio_complete >> emr_step_social >> check_emr_social_complete >> delete_emr >> complete >> failure_slack_notification

start_func >> failure_slack_notification >> complete

# start_func >> provision_emr >> check_emr >> emr_step_audio >>  emr_step_social >> check_emr_social_complete >> complete

# for i in range(1,len(ch_ls)):
     # emr_step_2 = steplist(ch_ls[i])
     # emr_step_1 >> emr_step_2
     # emr_step_1 = emr_step_2
# emr_step_2 >> check_emr_steps_complete >> delete_emr >> complete


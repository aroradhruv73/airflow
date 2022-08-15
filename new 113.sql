from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from lib.emr_launch_lib_v1 import AddEMRStepOperator, EMRStepSensor, CreateEMROperator, CreateEMRSensor, \
    TerminateEMROperator
from airflow.models import Variable
from airflow.operators.sensors import S3KeySensor
from lib.edt_utils import EDTAlerts, evaluate_dag_run
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

env = Variable.get("aws_environment") #'dev-private'
dag_name = 'tv_square_action_spot_historical_data'
aws_conn_id = f"ckp-etl-{env}"

bootstrap_script = f"s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/shell-script/bootstrap.sh"
emr_setup = {"num_core_nodes": "4", "master_instance_type": "m5.4xlarge", "core_instance_type": "r5.4xlarge"}

default_args = {
    'owner': 'ACDC',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 14),
    'email': ['darora@godaddy.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=8)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval='0 12 * * 3', catchup=False)

start = DummyOperator(dag=dag, task_id='start_task')

spark_cmd_action = f"""
   spark-submit --deploy-mode client --master yarn --conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/data_ingestion/tvsquareactionpostlog/tv_square_action_historical_data_upload.py -e {env}
   """
   
spark_cmd_spot = f"""
   spark-submit --deploy-mode client --master yarn --conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory s3://gd-ckpetlbatch-{env}-code/GDLakeDataProcessors/advertising-enablement/data_ingestion/tvsquareactionpostlog/tv_square_spot_historical_data_upload.py -e {env}
   """   


tsk_create_emr = CreateEMROperator(
    task_id='create_emr',
    aws_conn_id=aws_conn_id,
    emr_cluster_name=dag_name,
    num_core_nodes=emr_setup["num_core_nodes"],
    master_instance_type=emr_setup["master_instance_type"],
    core_instance_type=emr_setup["core_instance_type"],
    emr_sc_bootstrap_file_path=bootstrap_script,
    data_pipeline_name=dag_name,
    dag=dag,
)


tsk_create_emr_sensor = CreateEMRSensor(
    task_id='create_sensor',
    aws_conn_id=aws_conn_id,
    provisioned_record_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value')[0] }}",
    retries=1,
    dag=dag,
)

tsk_emr_step_spot = AddEMRStepOperator(
    task_id='spark_job_spot',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_sensor', key='job_flow_id') }}",
    step_cmd=spark_cmd_spot,
    step_name='spark_job_spot',
    dag=dag,
)

tsk_emr_step_sensor_spot = EMRStepSensor(
    task_id='tsk_emr_step_sensor_spot',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_sensor', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='spark_job_spot', key='return_value')[0] }}",
    dag=dag,
)


tsk_emr_step_action = AddEMRStepOperator(
    task_id='spark_job_action',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_sensor', key='job_flow_id') }}",
    step_cmd=spark_cmd_action,
    step_name='spark_job_action',
    dag=dag,
)

tsk_emr_step_sensor_action = EMRStepSensor(
    task_id='tsk_emr_step_sensor_action',
    aws_conn_id=aws_conn_id,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_sensor', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='spark_job_action', key='return_value')[0] }}",
    dag=dag,
)

tsk_terminate_emr = TerminateEMROperator(
    task_id='terminate_emr',
    aws_conn_id=aws_conn_id,
    provisioned_product_id="{{task_instance.xcom_pull(task_ids='create_emr', key='return_value')[1] }}",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

end = DummyOperator(dag=dag, task_id='end_task')

assess_dagrun_and_log_action = PythonOperator(
    task_id='evaluate_dag_run_tvsq_action_aws',
    provide_context=True,
    python_callable=evaluate_dag_run,
    op_kwargs={
        'send_success_email': True,
        'failure_email_recipients': 'darora@godaddy.com',
        'log_actionlog': False,
        'actionlog_db_name': 'enterprise',
        'aws_conn_id': aws_conn_id,
        'aws_environment': env
        },
    trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    retries=0,
    dag=dag,
)


start >> tsk_create_emr >> tsk_create_emr_sensor >> tsk_emr_step_spot >> tsk_emr_step_sensor_spot >> tsk_emr_step_action >> tsk_emr_step_sensor_action >> tsk_terminate_emr >> end >> assess_dagrun_and_log_action
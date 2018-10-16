import datetime as dt
import constants as c
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)

dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True
        # "email": "ewebbe@bol.com",
    },
)

dag2 = DAG(
    dag_id="my_second_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "ewebbe",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True
        # "email": "ewebbe@bol.com",
    },
)

def print_exec_date(**context):
    print(context["execution_date"])


# my_task = PythonOperator(
#     task_id="task_name",
#     python_callable=print_exec_date,
#     provide_context=True,
#     dag=dag
# )

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="CollectDataFromPgrs",
    postgres_conn_id='Training_postgres',
    sql="SELECT * \
         FROM land_registry_price_paid_uk \
         WHERE transfer_date = '{{ ds }}'",
    bucket='airflow_training_data_123',
    filename='PricePaid/{{ds}}/out.json',
    provide_context=True,
    dag=dag
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id='CreateTheCluster',
    cluster_name='analyse-pricing-{{ ds }}',
    project_id=c.PROJECT_ID,
    num_workers=2,
    zone='europe-west4-a',
    dag=dag2
)

compute_aggregates = DataProcPySparkOperator(
    task_id='ComputeAllTheThings',
    main='gs://europe-west1-training-airfl-9b3d38b2-bucket/other'
         ''
         '/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag2
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id='KillTheCluster',
    cluster_name='analyse-pricing-{{ ds }}',
    project_id=c.PROJECT_ID,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag2
)

dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
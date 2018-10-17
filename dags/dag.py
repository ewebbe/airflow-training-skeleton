import datetime as dt
import constants as c
import airflow
from airflow import DAG
# from airflow.hooks.http_hook import HttpHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
# from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
# import random

# class HttpToGcsOperator(BaseOperator):
#     """
#     Calls an endpoint on an HTTP system to execute an action
#
#     :param http_conn_id: The connection to run the operator against
#     :type http_conn_id: string
#     :param endpoint: The relative part of the full url. (templated)
#     :type endpoint: string
#     :param gcs_path: The path of the GCS to store the result
#     :type gcs_path: string
#     """
#
#     template_fields = ('endpoint', 'gcs_path', 'data')
#     template_ext = ()
#     ui_color = '#f4a460'
#
#     @apply_defaults
#     def __init__(self,
#                  endpoint,
#                  http_conn_id='default_http',
#                  gcs_conn_id="gcs_default",
#                  gcs_path=None,
#                  method="GET",
#                  *args,
#                  **kwargs):
#         super(HttpToGcsOperator, self).__init__(*args, **kwargs)
#         self.http_conn_id = http_conn_id,
#         self.endpoint = endpoint,
#         self.method = method,
#         self.gcs_path = gcs_path,
#         self.gcs_conn_id = gcs_conn_id
#
#     def execute(self, context, url=None):
#         # connect to HTTP and get data
#         http = HttpHook(
#             method='GET',
#             http_conn_id='http_default'
#         )
#         res = http.run(url, data=None, headers=None, extra_options=None)
#
#         temp_file = open("testfile.txt", "w")
#         temp_file.write(res.text)
#         temp_file.close()
#
#         # store to GCS
#         store = GoogleCloudStorageHook(
#             google_cloud_storage_conn_id='google_cloud_default',
#             delegate_to=None
#         )
#         store.upload(bucket='airflow_training_data_123',
#                      object='Currencies/{{ds}}/out.json',
#                      filename='testfile.txt',
#                      mime_type='application/json')

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


dag3 = DAG(
    dag_id="my_third_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "ewebbe",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True
        # "email": "ewebbe@bol.com",
    },
)

# dag4 = DAG(
#     dag_id="my_fourth_dag",
#     schedule_interval="30 7 * * *",
#     default_args={
#         "owner": "ewebbe",
#         "start_date": dt.datetime(2018, 10, 1),
#         "depends_on_past": True,
#         "email_on_failure": True
#         # "email": "ewebbe@bol.com",
#     },
# )

dag5 = DAG(
    dag_id="my_fifth_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "ewebbe",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True
        # "email": "ewebbe@bol.com",
    },
)


dag6 = DAG(
    dag_id="my_branching_dag1",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "ewebbe",
        "start_date": airflow.utils.dates.days_ago(21),
        "depends_on_past": True,
        "email_on_failure": True
        # "email": "ewebbe@bol.com",
    },
)

def print_exec_date(**context):
    print(context["execution_date"])


def print_exec_dayname(**context):
    return context["execution_date"].strftime("%A")

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
    main='gs://europe-west1-training-airfl-9b3d38b2-bucket/other/build_statistics.py',
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

copy_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='CopyDataToBigQuery',
    bucket='airflow_training_data_123',
    source_objects=['average_prices/{{ ds }}/*.parquet'],
    destination_project_dataset_table='Analysis.average_prices',
    source_format='PARQUET',
    write_disposition='WRITE_APPEND',
    dag=dag3
)

delete_from_bq = BigQueryOperator(
    task_id='delete_rows_from_bq',
    bql="DELETE FROM Analysis.average_prices WHERE trans_date = '{{ ds }}'",
    use_legacy_sql=False,
    dag=dag3
)

# collect_from_http = HttpToGcsOperator(
#     task_id='CollectFormHTTP',
#     conn_id='http_default',
#     url='"https://europe-west1-gdd-airflow-training.cloudfunctions.net/\
#           airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR"',
#     project_id=c.PROJECT_ID,
#     bucket='airflow_training_data_123',
#     filename='Currencies/{{ds}}/out.json',
#     dag=dag4
# )
#
# delete_from_bq_json = BigQueryOperator(
#     task_id='delete_rows_from_bqJSON',
#     bql="DELETE FROM Analysis.exchange_rates WHERE trans_date = '{{ ds }}'",
#     use_legacy_sql=False,
#     dag=dag4
# )
#
# copy_to_bq_json = GoogleCloudStorageToBigQueryOperator(
#     task_id='CopyDataToBigQueryJSON',
#     bucket='airflow_training_data_123',
#     source_objects=['Currencies/{{ ds }}/*.json'],
#     destination_project_dataset_table='Analysis.exchange_rates',
#     source_format='JSON',
#     write_disposition='WRITE_APPEND',
#     dag=dag4
# )

load_into_bigquery = DataFlowPythonOperator(
    task_id='Dataflow_into_bigquery',
    dataflow_default_options={"input": "gs://airflow_training_data_123/PricePaid/*/*.json",
                              "table": "Dataflow_import",
                              "dataset": "Analysis",
                              "project": c.PROJECT_ID,
                              "bucket": "europe-west1-training-airfl-22519ec9-bucket",
                              "name": "write-to-bq-{{ ds }}"},
    py_file="gs://airflow_training_data/other/dataflow_job.py",
    project_id=c.PROJECT_ID,
    dag=dag5
)

dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
delete_from_bq >> copy_to_bq
# collect_from_http >> delete_from_bq_json >> copy_to_bq_json


options = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
today = dt.datetime.now().strftime("%A")


def get_dayname():
    dt.datetime.now().strftime("%A")


branching = BranchPythonOperator(
    task_id='branch',
    python_callable=get_dayname,
    dag=dag6
)

join = DummyOperator(
    task_id='join',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag6
)

for option in options:
    branching >> DummyOperator(task_id=option, dag=dag6) >> join
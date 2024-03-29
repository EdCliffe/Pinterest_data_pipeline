from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Ed',
    'depends_on_past': False,
    'email': ['ed.cliffe1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(20202, 7, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 7, 30),
}

with DAG(dag_id='pin_dag',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator

    # Spark-submit - Run S3 to spark connector, stores in cassandra
    spark_batch = BashOperator(
        task_id='s3_to_spark_connector',
        bash_command="spark-submit --packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 ~/Work/Packages/Pinterest_data_pipeline/API/s3_spark_cassandra.py",
        dag=dag)
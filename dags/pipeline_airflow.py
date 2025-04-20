from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'cheick',
    'start_date': datetime(2025, 4, 20),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_vehicules_spark',
    default_args=default_args,
    schedule_interval=None,  # pas de planification automatique
    catchup=False,
    description='DAG pour lancer le pipeline PySpark de détection des anomalies véhicules',
) as dag:

    run_spark_submit = BashOperator(
        task_id='run_spark_pipeline',
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 /app/scripts/car_pipeline_spark.py"
    )
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2019, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'Ecommerce_Daily_Batch_Pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *', # 매일 새벽 2시
    catchup=False
) as dag:

    # 1. Spark 스트리밍이 어제 날짜를 마감했는지 S3 파일 확인
    wait_for_marker = S3KeySensor(
        task_id='wait_for_streaming_marker',
        bucket_name='ecommerce-datalake',
        bucket_key='status/streaming_done_{{ ds }}.done',
        aws_conn_id='aws_default',
        poke_interval=600,
        timeout=7200
    )

    # 2. 배치 집계 및 Redis 업데이트 실행
    run_batch = SparkSubmitOperator(
        task_id='execute_batch_aggregation',
        application='/opt/airflow/spark/batch_agg_job.py',
        conn_id='spark_default',
        application_args=[
            "{{ execution_date.year }}",
            "{{ execution_date.strftime('%m') }}",
            "{{ execution_date.strftime('%d') }}"
        ],
        packages="io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4"
    )

    wait_for_marker >> run_batch
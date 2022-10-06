from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from plugins import dataflow, upload_to_gs


default_args = {
    'owner' : 'ayyoub',
    'depends_on_past' : True,
    'start_date' : datetime(2021, 9, 13),
    'sla': timedelta(days=7)
}

schedule_interval = '15 2 * * 1'

with DAG('page_capture_history', default_args=default_args, schedule_interval=schedule_interval, catchup=False) as dag:
    upload_gs = PythonOperator(
        task_id=f"upload_gs",
        python_callable=upload_to_gs.upload_to_bucket,
        op_kwargs={
            'bucket_name': 'gs://output'
        }
    )
    
    run_dataflow = PythonOperator(
        task_id=f"run_dataflow",
        python_callable=dataflow.run()
    )

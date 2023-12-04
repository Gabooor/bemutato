from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import time
import os

def func_compare_folder_sizes(**kwargs):
    old_folder = kwargs['dag_run'].conf['old_moved_folder']
    new_folder = kwargs['dag_run'].conf['new_moved_folder']

    old_folder_size = kwargs['dag_run'].conf['old_folder_size']
    new_folder_size = kwargs['dag_run'].conf['new_folder_size']

    print(f"The folder {old_folder} was renamed to {new_folder}")
    if old_folder_size != new_folder_size:
        print(f"{old_folder} was {old_folder_size} bytes")
        print(f"{new_folder} is {new_folder_size} bytes")
        print(f"Starting folder monitoring on {new_folder}")

        # Triggers the on_created DAG
        base_execution_date = kwargs['execution_date']
        context = kwargs
        context['dag_run_conf'] = {'created_folder': new_folder}
        execution_date = base_execution_date + timedelta(seconds=5)
        trigger_dagrun = TriggerDagRunOperator(
            task_id=f"trigger_dag",
            trigger_dag_id='dag_on_created',
            conf=context['dag_run_conf'],
            execution_date=execution_date,
            dag=kwargs['dag']
        )
        trigger_dagrun.execute(context)
    
    else:
        print(f"The folder's size did not change, stopping DAG.")
    return


with DAG('dag_on_moved', start_date=datetime(2023,11,1), schedule_interval=None, catchup=False) as dag:
    
    task_monitor_folder = PythonOperator(
        task_id='print_folder',
        python_callable=func_compare_folder_sizes,
        provide_context=True,
    )

    task_monitor_folder
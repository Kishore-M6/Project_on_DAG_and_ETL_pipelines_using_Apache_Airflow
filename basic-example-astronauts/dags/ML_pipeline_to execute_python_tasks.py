from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


#  DAG pipeline tasks
def preprocess_data_task():
    print("task-1 preprocessing the data ........")
def training_data_task():
    print("task-2 training the data ........")
def evaluate_models_task():
    print("task-3 evaluate models created ..........")
def monitoring_models_task():
    print("task-4 monitoring the models for syncing updates.........")

with DAG(
    'ML_pipeline_to_execute_python_tasks',
    start_date=datetime(2025, 1, 1),
    schedule="@weekly"
) as dag:
    # define the tasks execution order
    preprocessdata=PythonOperator(task_id='preprocess-data-task',python_callable=preprocess_data_task)
    trainingdata=PythonOperator(task_id='training-data-task',python_callable=training_data_task)
    evaluatemodelsdata=PythonOperator(task_id='evaluate-models-task',python_callable=evaluate_models_task)
    monitoringmodelsdata=PythonOperator(task_id='monitoring-models-task',python_callable=monitoring_models_task)

    # task dependencies
    preprocessdata >> trainingdata >> evaluatemodelsdata >> monitoringmodelsdata
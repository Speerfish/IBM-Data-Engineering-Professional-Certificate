# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#Task 1 - Define the DAG arguments

dag_args = {
    'owner': 'Random_Dude',
    'start_date': days_ago(0),
    'email': ['Raandom_Dudeee@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#Task 2 - Define the DAG

dag = DAG(
    dag_id='process_web_log',
    default_args=dag_args,
    description='Data Pipelines Using Apache AirFlow',
    schedule_interval=timedelta(days=1),
)

#Task 3 - Create a task to extract data

extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 $AIRFLOW_HOME/dags/capstone/accesslog.txt > \
    $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
    dag=dag,
)

# Task 4 - Create a task to transform the data in the txt file

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep 198.46.149.143 $AIRFLOW_HOME/dags/capstone/extracted_data.txt > \
    $AIRFLOW_HOME/data/transformed_data.txt',
    dag=dag,
)

# Task 5 - Create a task to load the data

load_data = BashOperator(
        task_id='load_data',
        bash_command='tar -cvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
        dag=dag
)

# Task 6 - Define the task pipeline

extract_data >> transform_data >> load_data
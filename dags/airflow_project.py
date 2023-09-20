from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
import json
import requests
import pandas as pd


def save_posts(ti):
    base_url = Variable.get("basketball_api") 
    all_posts = []

    page = 1
    max_pages = 50

    pages_retrieved = 0

    while pages_retrieved < max_pages:
        response = requests.get(f'{base_url}/players/?page={page}')
        response_data = response.json()
        posts = response_data.get('data', [])

        if not posts:
            break

        all_posts.extend(posts)

        # Check if there is a next page
        if response_data['meta']['next_page']:
            page += 1
            pages_retrieved += 1
        else:
            break

    with open('/home/bipin/airflow/data/basketball.json', 'w') as f:
        json.dump(all_posts, f)


def json_to_csv():
    df = pd.read_json('/home/bipin/airflow/data/basketball.json')
    df_team_flattened = pd.json_normalize(df['team'])
    df_team_flattened.rename(columns={'id':'team_id'},inplace=True)
    df = df.drop(columns='team')
    df = pd.concat([df, df_team_flattened], axis=1)
    df = df.fillna(0)
    df = df.drop('position', axis='columns')

    df.to_csv('/home/bipin/airflow/data/basketball.csv', index=False)


with DAG(
    dag_id = "airflow_project",
    schedule_interval='@daily',
    start_date=datetime(2023,9,17),
    catchup=False
) as dag:
    
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='airflow_project',
        endpoint='players/'
    )

    task_get_posts = SimpleHttpOperator(
        task_id = 'get_posts',
        http_conn_id='airflow_project',
        endpoint='players/',
        method='GET',   
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_save = PythonOperator(
        task_id = 'save_posts',
        python_callable=save_posts
    )

    

    task_convert_to_csv = PythonOperator(
        task_id = 'convert_to_csv',
        python_callable=json_to_csv
    )

    file_sensing_initial = FileSensor(
        task_id="file_sensing_for_initial_csv_converter",
        filepath="/home/bipin/airflow/data/basketball.csv",
        poke_interval= 20,
        mode="poke"           
    )

    move_file_task = BashOperator(
        task_id='move_file_to_tmp',
        bash_command='mv /home/bipin/airflow/data/basketball.csv /tmp/',  # Replace with the source file path
    )

    file_sensing_temp = FileSensor(
        task_id="file_sensing_in_tmp_folder",
        filepath="/tmp/basketball.csv",
        poke_interval= 20,
        mode="poke"            
    )

    task_load_to_postgres = PostgresOperator(
        task_id='load_to_postgres',
        postgres_conn_id='airflow_postgres',  # Replace with your Postgres connection ID
        sql="""
        COPY basketball_data(id,first_name,height_feet,height_inches,last_name,weight_pounds,team_id,abbreviation,city,conference,division,full_name,name) FROM '/tmp/basketball.csv' CSV HEADER;
        """
    )

    spark_submit = BashOperator(
        task_id='spark_submit_task',
        bash_command="spark-submit /home/bipin/sparkproject/basketball-spark.py",
    )

    read_table = PostgresOperator(
        sql = "select * from solution_df",
        task_id = "read_table_task",
        postgres_conn_id = "airflow_postgres",
        autocommit=True
    )

    def process_postgres_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='read_table_task')
        # Process the result data here
        for row in result:
            print(row)

    process_data_task = PythonOperator(
        task_id='process_postgres_result',
        python_callable=process_postgres_result,
        provide_context=True
    )
   

    task_is_api_active >> task_get_posts >> task_save >> task_convert_to_csv >> file_sensing_initial >> move_file_task >> file_sensing_temp >> task_load_to_postgres >> spark_submit >> read_table >> process_data_task
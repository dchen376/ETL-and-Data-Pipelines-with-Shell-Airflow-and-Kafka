from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'user1',  
    'start_date': datetime.today(),
    'email': 'user1@example.com',  
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    DAG_id ="ETL_toll_data",
    Schedule ="@daily",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    catchup=False
) as dag:


    unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="tar -xvf ~/airflow/dags/finalassignment/tolldata.tgz -C ~/airflow/dags/finalassignment/staging/"
    )

    extract_data_from_csv = BashOperator(
        task_id="extract_data_from_csv",
        bash_command="cut -d',' -f1,2,3,4 ~/airflow/dags/finalassignment/staging/vehicle-data.csv > ~/airflow/dags/finalassignment/staging/csv_data.csv"
    )

    extract_data_from_tsv = BashOperator(
        task_id="extract_data_from_tsv",
        bash_command="cut -f5,6,7 ~/airflow/dags/finalassignment/staging/tollplaza-data.tsv > ~/airflow/dags/finalassignment/staging/tsv_data.csv"
    )

    extract_data_from_fixed_width = BashOperator(
        task_id="extract_data_from_fixed_width",
        bash_command="cut -c59-62,63-67 ~/airflow/dags/finalassignment/staging/payment-data.txt > ~/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    )


    consolidate_data = BashOperator(
        task_id="consolidate_data",
        bash_command="paste -d',' ~/airflow/dags/finalassignment/staging/csv_data.csv ~/airflow/dags/finalassignment/staging/tsv_data.csv ~/airflow/dags/finalassignment/staging/fixed_width_data.csv > ~/airflow/dags/finalassignment/staging/extracted_data.csv"
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="awk 'BEGIN{FS=OFS=\",\"} { $4=toupper($4); print }' ~/airflow/dags/finalassignment/staging/extracted_data.csv > ~/airflow/dags/finalassignment/staging/transformed_data.csv"
    )


    # task dependencies
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width
    extract_data_from_fixed_width >> consolidate_data >> transform_data    
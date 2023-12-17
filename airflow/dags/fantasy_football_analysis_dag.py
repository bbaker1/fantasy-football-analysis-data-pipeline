import sys
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from scripts.get_data.update_week import update_week
from scripts.get_data.get_fantasy_data_delta import get_fantasy_data
from scripts.get_data.get_scores_delta import get_scores_data
from scripts.get_data.get_schedules import get_schedule_data

dt = datetime.now()
year = dt.year
# /usr/local/airflow/dags/...

with DAG(
    dag_id='fantasy_football_analysis_dag',
    start_date=datetime(2023, 11, 27),
    schedule_interval='0 7 * * 2',
    catchup=False,
) as dag:
    

    
    # Use bash operator to run script to save locally then upload to gcs operator to pick up local file
    extract_fantasy_qb_data = PythonOperator(
        task_id='extract_fantasy_qb_data',
        python_callable=get_fantasy_data,
        op_kwargs={'position': 'qb', 'year': year}
    )

    extract_fantasy_rb_data = PythonOperator(
        task_id='extract_fantasy_rb_data',
        python_callable=get_fantasy_data,
        op_kwargs={'position': 'rb', 'year': year}
    )

    extract_fantasy_wr_data = PythonOperator(
        task_id='extract_fantasy_wr_data',
        python_callable=get_fantasy_data,
        op_kwargs={'position': 'wr', 'year': year}
    )

    extract_fantasy_te_data = PythonOperator(
        task_id='extract_fantasy_te_data',
        python_callable=get_fantasy_data,
        op_kwargs={'position': 'te', 'year': year}
    )

    extract_fantasy_td_data = PythonOperator(
        task_id='extract_fantasy_td_data',
        python_callable=get_fantasy_data,
        op_kwargs={'position': 'td', 'year': year}
    )

    extract_scores_data = PythonOperator(
        task_id='extract_scores_data',
        python_callable=get_scores_data,
        op_kwargs={'year': year}
    )

    extract_schedule_data = PythonOperator(
        task_id='extract_schedule_data',
        python_callable=get_schedule_data,
        op_kwargs={'year': year}
    )


    load_fantasy_qb_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_fantasy_qb_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_qb.csv',
        dst=f'fantasy/qb/{year}/{dt.month}/{dt.day}/qb_fantasy_data.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_fantasy_rb_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_fantasy_rb_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_rb.csv',
        dst=f'fantasy/rb/{year}/{dt.month}/{dt.day}/rb_fantasy_data.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_fantasy_wr_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_fantasy_wr_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_wr.csv',
        dst=f'fantasy/wr/{year}/{dt.month}/{dt.day}/wr_fantasy_data.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_fantasy_te_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_fantasy_te_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_te.csv',
        dst=f'fantasy/te/{year}/{dt.month}/{dt.day}/te_fantasy_data.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_fantasy_td_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_fantasy_td_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/fantasy/fantasy_td.csv',
        dst=f'fantasy/td/{year}/{dt.month}/{dt.day}/td_fantasy_data.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_scores_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_scores_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/scores/scores.csv',
        dst=f'scores/{year}/{dt.month}/{dt.day}/scores.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_schedule_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id='load_schedule_data_to_gcs',
        src='/usr/local/airflow/dags/scripts/get_data/data/schedules/schedule.csv',
        dst=f'schedules/{year}/{year}_schedule.csv',
        bucket='fantasy-football-data-bucket'
    )

    load_fantasy_qb_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fantasy_qb_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'fantasy/qb/{year}/{dt.month}/{dt.day}/qb_fantasy_data.csv',
        destination_project_dataset_table='raw.fantasy_qb',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_fantasy_rb_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fantasy_rb_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'fantasy/rb/{year}/{dt.month}/{dt.day}/rb_fantasy_data.csv',
        destination_project_dataset_table='raw.fantasy_rb',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_fantasy_wr_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fantasy_wr_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'fantasy/wr/{year}/{dt.month}/{dt.day}/wr_fantasy_data.csv',
        destination_project_dataset_table='raw.fantasy_wr',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_fantasy_te_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fantasy_te_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'fantasy/te/{year}/{dt.month}/{dt.day}/te_fantasy_data.csv',
        destination_project_dataset_table='raw.fantasy_te',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_fantasy_td_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fantasy_td_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'fantasy/td/{year}/{dt.month}/{dt.day}/td_fantasy_data.csv',
        destination_project_dataset_table='raw.fantasy_td',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_scores_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_scores_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'scores/{year}/{dt.month}/{dt.day}/scores.csv',
        destination_project_dataset_table='raw.scores',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    load_schedule_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_schedule_data_to_bigquery',
        bucket='fantasy-football-data-bucket',
        source_objects=f'schedules/{year}/{year}_schedule.csv',
        destination_project_dataset_table='raw.schedules',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    update_week = PythonOperator(
        task_id='update_week',
        python_callable=update_week
    )

    extract_fantasy_qb_data >> load_fantasy_qb_data_to_gcs >> load_fantasy_qb_data_to_bigquery >> update_week
    extract_fantasy_rb_data >> load_fantasy_rb_data_to_gcs >> load_fantasy_rb_data_to_bigquery >> update_week
    extract_fantasy_wr_data >> load_fantasy_wr_data_to_gcs >> load_fantasy_wr_data_to_bigquery >> update_week
    extract_fantasy_te_data >> load_fantasy_te_data_to_gcs >> load_fantasy_te_data_to_bigquery >> update_week
    extract_fantasy_td_data >> load_fantasy_td_data_to_gcs >> load_fantasy_td_data_to_bigquery >> update_week
    extract_scores_data >> load_scores_data_to_gcs >> load_scores_data_to_bigquery >> update_week
    extract_schedule_data >> load_schedule_data_to_gcs >> load_schedule_data_to_bigquery >> update_week

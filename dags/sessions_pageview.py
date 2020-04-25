import logging
import os 

import airflow
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': airflow.utils.dates.days_ago(0),
    'schedule_interval': '@hourly'
}

dag = DAG(
    'sessions_pageview',
    default_args=default_args,
    description='DAG to read latest data from source and update into target tables'
    )

sqlDir = 'SQL/'

# [START delta_load_lock_ts]
delta_load_lock_ts = BigQueryOperator(
    task_id='delta_load_lock_ts',
    sql=sqlDir+'delta_load_lock_ts.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END delta_load_lock_ts]

# [START load stage_dim_content]
load_stg_dim_content = BigQueryOperator(
    task_id='load_stg_dim_content',
    sql=sqlDir+'load_stg_dim_content.sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table='stg_dim_content',
    dag=dag)
# [END load stage_dim_content]

# [START load dim_content]
load_dim_content = BigQueryOperator(
    task_id='load_dim_content',
    sql=sqlDir+'load_dim_content.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END load dim_content]

# [START load source_stage_sessions_pageview]
load_source_stg_session_pageview = BigQueryOperator(
    task_id='load_source_stg_session_pageview',
    sql=sqlDir+'load_source_stg_session_pageview.sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table='stg_session_pageview',
    dag=dag)
# [END load source_stage_sessions_pageview]

# [START source_stage_sessions_pageview]
load_dim_page = BigQueryOperator(
    task_id='load_dim_page',
    sql=sqlDir+'load_dim_page.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END source_stage_sessions_pageview]

# [START load_dim_stage_sessions]
load_dim_stage_sessions = BigQueryOperator(
    task_id='load_dim_stage_sessions',
    sql=sqlDir+'load_dim_stage_sessions.sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table='stg_dim_session',
    dag=dag)
# [END load_dim_stage_sessions]

# [START load_dim_sessions]
load_dim_sessions = BigQueryOperator(
    task_id='load_dim_sessions',
    sql=sqlDir+'load_dim_sessions.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END load_dim_sessions]

# [START load_stg_page_view_by_session]
load_stg_page_view_by_session = BigQueryOperator(
    task_id='load_stg_page_view_by_session',
    sql=sqlDir+'load_stg_page_view_by_session.sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table='stg_page_view_by_session',
    dag=dag)
# [END load_stg_page_view_by_session]

# [START load_page_view_by_session]
load_page_view_by_session = BigQueryOperator(
    task_id='load_page_view_by_session',
    sql=sqlDir+'load_page_view_by_session.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END load_page_view_by_session]

# [START delta_load_unlock_ts]
delta_load_unlock_ts = BigQueryOperator(
    task_id='delta_load_unlock_ts',
    sql=sqlDir+'delta_load_unlock_ts.sql',
    use_legacy_sql=False,
    destination_dataset_table=None,
    dag=dag)
# [END delta_load_unlock_ts]

delta_load_lock_ts >> load_stg_dim_content >> load_dim_content >> load_source_stg_session_pageview
load_source_stg_session_pageview >> load_dim_page >> load_dim_stage_sessions >> load_dim_sessions
load_dim_sessions >> load_stg_page_view_by_session >> load_page_view_by_session >> delta_load_unlock_ts

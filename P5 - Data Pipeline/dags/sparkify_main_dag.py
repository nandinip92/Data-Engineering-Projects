from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

#'depends_on_past': False,
#'retries': 3,
#'retry_delay': timedelta(minutes=5),
#'catchup_by_default': False,
#'email_on_retry': False

dag = DAG('sparkify_main_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly",
          template_searchpath='/home/workspace/airflow/',
          max_active_runs=1,
          catchup = False
        )
#'0 * * * *'
#start_date=datetime(2018, 1, 1, 0, 0, 0, 0),
#end_date=datetime(2018, 12, 1, 0, 0, 0, 0),


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_sparkify_tables = PostgresOperator(
    task_id = "create_sparkify_tables",
    dag = dag,
    postgres_conn_id = "redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key = "log_data",
    is_partitioned = False,
    region = "us-west-2",
    json_format_option = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key = "song_data/A/A/A",
    region = "us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table = "songplays",
    select_sql_statement = SqlQueries.songplay_table_insert,
    insert_mode = "append"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table = "users",
    select_sql_statement = SqlQueries.user_table_insert,
    insert_mode = "insert_delete",
    primary_key = "userid"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table = "songs",
    select_sql_statement = SqlQueries.song_table_insert,
    insert_mode = "append"
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table = "artists",
    select_sql_statement = SqlQueries.artist_table_insert,
    insert_mode = "append"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table = "time",
    select_sql_statement = SqlQueries.time_table_insert,
    insert_mode = "append"
)

dimension_tables = ['songs','artists','time','users']
tests = [{"test": "SELECT count(*) FROM songs","expected":24},
        {"test": "SELECT count(*) FROM artists","expected":24},
        {"test": "SELECT count(*) FROM time","expected":6820},
        {"test": "SELECT count(*) FROM users","expected":104},]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table = "songplays",
    dim_tables = dimension_tables,
    tests = tests
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>> create_sparkify_tables
create_sparkify_tables >> stage_events_to_redshift
create_sparkify_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

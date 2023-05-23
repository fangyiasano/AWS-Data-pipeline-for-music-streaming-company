from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.final_project_sql_statements import SqlQueries

# Define the default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2023, 5, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

# Define the DAG using the @dag decorator
@dag(default_args=default_args, schedule_interval='@hourly', description='Load and transform data in Redshift with Airflow')
def final_project():

    # Define the start operator
    start_operator = EmptyOperator(task_id='Begin_execution')

    # Define the stage_events_to_redshift operator
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_option="s3://udacity-dend/log_json_path.json",
        table="staging_events"
    )

    # Define the stage_songs_to_redshift operator
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        json_option="auto",
        table="staging_songs"
    )

    # Define the load_songplays_table operator
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert,
        table="songplays"
    )

    # Define the load_user_dimension_table operator
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.user_table_insert,
        table="users",
        truncate_table=True
    )

    # Define the load_song_dimension_table operator
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.song_table_insert,
        table="songs",
        truncate_table=True
    )

    # Define the load_artist_dimension_table operator
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.artist_table_insert,
        table="artists",
        truncate_table=True
    )

    # Define the load_time_dimension_table operator
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.time_table_insert,
        table="time",
        truncate_table=True
    )

    # Define the run_quality_checks operator
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        test_query='SELECT COUNT(*) FROM songplays',
        expected_result=0
    )

    # Define the end operator
    end_operator = EmptyOperator(task_id='Stop_execution')

    # Set up the task dependencies
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

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

# Create an instance of the final_project DAG
final_project_dag = final_project()

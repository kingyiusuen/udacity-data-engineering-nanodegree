from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries


AWS_CONN_ID = "aws"
REDSHIFT_CONN_ID = "redshift"


default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_retry": False,
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *"
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket="udacity-dend",
        s3_key="log-data",
        table="staging_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket="udacity-dend",
        s3_key="song-data",
        table="staging_songs",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="users",
        sql=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songs",
        sql=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="artists",
        sql=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="time",
        sql=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id=REDSHIFT_CONN_ID,
        tables=["songplays", "users", "songs", "artists", "time"],
    )
    
    end_operator = DummyOperator(task_id="Stop_execution")
    
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()

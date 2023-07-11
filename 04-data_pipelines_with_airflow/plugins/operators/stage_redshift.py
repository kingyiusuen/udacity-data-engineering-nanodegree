from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = "#358140"

    @apply_defaults
    def __init__(self,
                 aws_conn_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        copy_sql = f"""
        COPY {self.table}
        FROM "{s3_path}"
        ACCESS_KEY_ID "{credentials.access_key}"
        SECRET_ACCESS_KEY "{credentials.secret_key}"
        FORMAT AS JSON "auto"
        """
        redshift.run(copy_sql)

        self.log.info(f"Success: Copying data from S3 to Redshift table {self.table}")
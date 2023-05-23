from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 json_option="auto",
                 *args, **kwargs):
        """
        Initializes the StageToRedshiftOperator.

        Args:
            aws_credentials_id (str): The connection ID for AWS credentials in Airflow.
            redshift_conn_id (str): The connection ID for Redshift in Airflow.
            s3_bucket (str): The name of the S3 bucket.
            s3_key (str): The S3 key or prefix for the files to be loaded.
            table (str): The name of the target table in Redshift.
            json_option (str): The JSON format option for copying the data.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_option = json_option

    def execute(self, context):
        """
        Executes the copying of data from S3 to Redshift.

        Args:
            context (dict): The context dictionary provided by Airflow.
        """
        aws_hook = AwsHook(aws_credentials_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_option
        )
        redshift.run(formatted_sql)

from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):
        """
        Initializes the DataQualityOperator.

        Args:
            redshift_conn_id (str): The connection ID for Redshift in Airflow.
            tests (list): A list of dictionaries representing the tests to be executed.
                Each dictionary should contain 'check_sql' (the SQL query to run) and
                'expected_result' (the expected result of the query).
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        """
        Executes the data quality checks.

        Args:
            context (dict): The context dictionary provided by Airflow.

        Raises:
            ValueError: If a data quality check fails.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql = test.get('check_sql')
            exp_result = test.get('expected_result')

            # Execute the SQL query
            records = redshift_hook.get_records(sql)

            # Check if the query returned any results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check has failed. {sql} returned no results")

            num_records = records[0][0]
            # Compare the actual result with the expected result
            if num_records != exp_result:
                raise ValueError(f"Data quality check has failed. {sql} returned {num_records} instead of {exp_result}")

            self.log.info(f"Data quality on SQL {sql} check passed with {records[0][0]} records")

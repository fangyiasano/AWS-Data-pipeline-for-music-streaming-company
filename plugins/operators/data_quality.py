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
             redshift_conn_id (str): The connection ID for Redshift configured in Airflow, used to set up the database connection.
             tests (list of dict): A list of dictionaries where each dictionary should contain:
                'check_sql' (str): The SQL query to be executed for data quality checks.
                 'expected_result' (int): The expected integer result of the SQL query.
             *args: Additional positional arguments passed to the base class.
             **kwargs: Additional keyword arguments passed to the base class.
         
         Examples:
             tests=[
                 {'check_sql': 'SELECT COUNT(*) FROM users WHERE active = TRUE', 'expected_result': 100},
                 {'check_sql': 'SELECT COUNT(*) FROM sales WHERE amount > 1000', 'expected_result': 10}
              ]
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        """
        Executes the data quality checks.

        Args:
            context (dict): The context dictionary provided by Airflow, which contains information like the execution date,
                    task instance, and other workflow-specific details that may be useful in data quality checks.

        Raises:
            ValueError: Raised if a data quality check fails, specifying which test failed and the mismatch between expected
                        and actual results if applicable. This aids in debugging data quality issues detected during task execution.
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

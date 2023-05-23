from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 truncate_table=False,
                 *args, **kwargs):
        """
        Initializes the LoadDimensionOperator.

        Args:
            redshift_conn_id (str): The connection ID for Redshift in Airflow.
            table (str): The name of the dimension table to load data into.
            select_sql (str): The SELECT statement to retrieve data for insertion into the dimension table.
            truncate_table (bool): If True, the dimension table will be truncated before loading data.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        Executes the loading of data into the dimension table.

        Args:
            context (dict): The context dictionary provided by Airflow.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading data into dimension table {self.table}")
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(formatted_sql)
        self.log.info(f"Data has loaded into dimension table {self.table}")

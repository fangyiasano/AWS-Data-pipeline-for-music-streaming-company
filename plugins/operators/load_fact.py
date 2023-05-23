from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):
        """
        Initializes the LoadFactOperator.

        Args:
            redshift_conn_id (str): The connection ID for Redshift in Airflow.
            table (str): The name of the fact table to load data into.
            select_sql (str): The SELECT statement to retrieve data for insertion into the fact table.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        """
        Executes the loading of data into the fact table.

        Args:
            context (dict): The context dictionary provided by Airflow.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data into fact table {self.table}")
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(formatted_sql)
        self.log.info(f"Data has loaded into fact table {self.table}")

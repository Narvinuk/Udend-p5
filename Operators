import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        

        
    def execute(self, context):
        self.log.info("Fetching the redshift hook..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Running Data Quality checks for table - {self.table}")
        
        # Check for zero rows
        for table in self.table:
            self.log.info(f"Running Data Quality checks for table - {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality test - {table} table - check for zero rows - FAILED. Returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality test - {table} table - check for zero rows - FAILED. Returned 0 rows")
            self.log.info(f"Data quality test - {table} table - check for zero rows - PASSED. {records[0][0]} records found")

       

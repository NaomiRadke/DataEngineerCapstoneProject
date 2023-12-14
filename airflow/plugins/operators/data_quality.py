from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 pkeys = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.pkeys = pkeys
        self.postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)


    def execute(self, context):
        self.log.info('Starting data checks')
        for i in range(len(self.tables)):
            
            table = self.tables[i]
            pkey = self.pkeys[i]
            
            records = self.postgres_hook.get_records(
                "SELECT COUNT(*) FROM {}".format(table)
            )
            
            # Check that records exists
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError( "{} returned no results".format(table))
            
            num_records = records[0][0]
            
            if num_records < 1:
                self.log.info(
                    "No records found in table {}!".format(table)
                )
                raise ValueError(
                    "No records found in table {}!".format(table)
                )
                
            self.log.info(
                "Data quality check on table {} passed with {} records".format(table, num_records)
            )
            
            # Check that there are no duplicate primary keys
            records_pkey = self.postgres_hook.get_records(
                "SELECT {} FROM {}".format(pkey, table)
            )
            
            # Create a dictionary to store the count of each record
            record_count = {}

            # Iterate through the records and count the occurrences
            for record in records_pkey:
                if record[0] in record_count:
                    record_count[record[0]] += 1
                else:
                    record_count[record[0]] = 1

            # Check for duplicates
            duplicates = [record for record, count in record_count.items() if count > 1]
            
            self.log.info("Are there any duplicates in table {}?{}".format(table, duplicates))

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for check in self.checks:
            records = redshift_hook.get_records(check['test_sql'])
            if check['comparison'] == '>':
                if not (records[0][0] > check['expected_result']):
                    raise ValueError(f"Data quality check failed on {check['test_sql']}. Returned: {records[0][0]} rows, "
                                     f"Expected {check['comparison']}{check['expected_result']} rows")
            elif check['comparison'] == '<':
                if not (records[0][0] < check['expected_result']):
                    raise ValueError(f"Data quality check failed on {check['test_sql']}. Returned: {records[0][0]} rows, "
                                     f"Expected {check['comparison']}{check['expected_result']} rows")
            else:
                if not (records[0][0] == check['expected_result']):
                    raise ValueError(f"Data quality check failed on {check['test_sql']}. Returned: {records[0][0]} rows, "
                                     f"Expected {check['comparison']}{check['expected_result']} rows")
            self.log.info(f"Data quality passed on {check['test_sql']} with {records[0][0]} records")
            
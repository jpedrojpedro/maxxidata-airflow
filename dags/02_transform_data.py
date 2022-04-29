import logging
import datetime as dt
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from utils.helper import apply_task_downstream


log = logging.getLogger(__name__)
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

sequence = []

dag = DAG(
    '02_transform_data',
    default_args=DEFAULT_ARGS,
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)

do_nothing = DummyOperator(
    task_id='do_nothing',
    trigger_rule=TriggerRule.ALL_DONE,  # all directly upstream tasks are finished - failed or succeeded
    dag=dag,
)
sequence.append(do_nothing)

apply_task_downstream(sequence)

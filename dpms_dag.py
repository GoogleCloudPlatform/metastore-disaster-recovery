# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime, os
import airflow
from airflow.operators import bash_operator
from airflow.sensors.python import PythonSensor

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

export_cmd = 'gcloud metastore services export gcs'
import_cmd = 'gcloud metastore services import gcs'

default_args = {
    'owner': 'DPMS Export and Import',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}


def _wait_for_instance_ready():
    instance_status_cmd = 'gcloud metastore services describe {dpms_standby_instance} --location={dpms_standby_region} | grep stateMessage:'.format(
        dpms_standby_instance=os.environ.get("DPMS_STANDBY_INSTANCE"),
        dpms_standby_region=os.environ.get("DPMS_STANDBY_REGION"))
    instance_status = os.popen(instance_status_cmd).read().rstrip()
    print('instance_status: ' + instance_status)
    if 'The service is being updated' in instance_status:
        print('instance is being updated')
        return False
    else:
        print('instance is ready')
        return True


with airflow.DAG(
        'dpms_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval='*/30 * * * *') as dag:
    # Export metadata from primary metastore
    dpms_export = bash_operator.BashOperator(
        task_id='dpms_export', 
        bash_command='{export_cmd} {dpms_primary_instance} --location={dpms_primary_region} --destination-folder=gs://{backup_path}'.format(
            export_cmd=export_cmd,
            dpms_primary_instance=os.environ.get("DPMS_PRIMARY_INSTANCE"),
            dpms_primary_region=os.environ.get("DPMS_PRIMARY_REGION"),
            backup_path=os.environ.get("BACKUP_BUCKET")))
    find_backup = bash_operator.BashOperator(
        task_id='find_backup', 
        bash_command='gsutil ls gs://{backup_bucket}/* | sort -k 1 | tail -1'.format(backup_bucket=os.environ.get("BACKUP_BUCKET")),
        do_xcom_push=True)
    wait_for_ready_status = PythonSensor(
       task_id="wait_for_ready_status",
       python_callable=_wait_for_instance_ready
    )
    current_ts = bash_operator.BashOperator(
        task_id='current_ts', 
        bash_command='date +"%Y-%m-%d-%H-%M-%S"',
        do_xcom_push=True)
    dpms_import = bash_operator.BashOperator(
        task_id='dpms_import', 
        bash_command='{import_cmd} {dpms_standby_instance} --location={dpms_standby_region} --dump-type=mysql --database-dump={backup_file} --import-id=import-{current_ts}'.format(
            import_cmd=import_cmd,
            dpms_standby_instance=os.environ.get("DPMS_STANDBY_INSTANCE"),
            dpms_standby_region=os.environ.get("DPMS_STANDBY_REGION"),
            backup_file='{{ ti.xcom_pull(task_ids="find_backup") }}',
            current_ts='{{ ti.xcom_pull(task_ids="current_ts") }}'))
    dpms_export >> find_backup >> wait_for_ready_status >> current_ts >> dpms_import

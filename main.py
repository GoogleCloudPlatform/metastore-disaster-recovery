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

import os, time
from flask import Flask

app = Flask(__name__)


@app.route("/export", methods=['POST'])
def run_export():
    dpms_primary_region = os.environ.get("DPMS_PRIMARY_REGION")
    backup_bucket = os.environ.get("BACKUP_BUCKET")
    backup_path = "gs://" + backup_bucket
    dpms_primary_instance = os.environ.get("DPMS_PRIMARY_INSTANCE")
    export_cmd = "gcloud metastore services export gcs " + dpms_primary_instance + " --location=" + dpms_primary_region + " --destination-folder=" + backup_path
    print("export_cmd: " + export_cmd)
    exit_code = os.system(export_cmd)
    print("export completed with exit code %d" % exit_code)
    return str(exit_code)


@app.route("/import", methods=['POST'])
def run_import():
    dpms_standby_region = os.environ.get("DPMS_STANDBY_REGION")
    backup_bucket = os.environ.get("BACKUP_BUCKET")
    dpms_standby_instance = os.environ.get("DPMS_STANDBY_INSTANCE")
    gsutil_cmd = "gsutil ls gs://" + backup_bucket + "/* | sort -k 1 | tail -1"
    last_backup = os.popen(gsutil_cmd).read().rstrip()
    print("last_backup: " + last_backup)

    # generate a unique import id 
    date_cmd = "date +'%Y-%m-%d-%H-%M-%S'"
    import_id = "import-" + os.popen(date_cmd).read().rstrip()
    print("import_id: " + import_id)
    
    while True:
        instance_status_cmd = "gcloud metastore services describe " + dpms_standby_instance + " --location=" + dpms_standby_region + " | grep stateMessage:"
        instance_status = os.popen(instance_status_cmd).read().rstrip()
        print("instance_status: " + instance_status)
        if "The service is being updated" in instance_status:
            print("sleeping for 1 minute")
            time.sleep(60)
        else:
            break
    
    import_cmd = "gcloud metastore services import gcs " + dpms_standby_instance + " --location=" + dpms_standby_region + " --dump-type=mysql --database-dump=" + last_backup + " --import-id=" + import_id  
    print("import_cmd: " + import_cmd)
    exit_code = os.system(import_cmd)
    print("import completed with exit code %d" % exit_code)
    return str(exit_code)


if __name__ == "__main__":
    app.run()

# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from databricks_cli.sdk import JobsService


class JobsApi(object):
    def __init__(self, api_client):
        self.client = JobsService(api_client)

    def create_job(self, json):
        return self.client.client.perform_query('POST', '/jobs/create', data=json)

    def list_jobs(self):
        return self.client.list_jobs()

    def delete_job(self, job_id):
        return self.client.delete_job(job_id)

    def get_job(self, job_id):
        return self.client.get_job(job_id)

    def update_job(self, job_id):
        return self.client.update_job(job_id)

    def reset_job(self, json):
        return self.client.client.perform_query('POST', '/jobs/reset', data=json)

    def run_now(self, job_id, jar_params=None, notebook_params=None, python_params=None, spark_submit_params=None):
        return self.client.run_now(job_id, jar_params, notebook_params, python_params,
                                   spark_submit_params)

    def has_job(self, full_job_name):
        jobs = self.list_jobs().get('jobs', [])
        result = list(filter(lambda job: job['settings']['name'] == full_job_name, jobs))
        return len(result) != 0

    def get_job_by_name(self, full_job_name):
        jobs = self.list_jobs()['jobs']
        result = list(filter(lambda job: job['settings']['name'] == full_job_name, jobs))
        if not result:
            print('Job not found: %s' % full_job_name)
            #raise Exception('Cannot not find job: %s' % full_job_name)
            return None
        elif len(result) > 1:
            print('Multiple jobs with same name: %s' % full_job_name)
            #raise Exception('Multiple jobs with the same name: %s' % full_job_name)
            return None
        else:
            return result[0]
            
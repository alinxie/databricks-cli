# Databricks CLI
# Copyright 2018 Databricks, Inc.
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

import os
import json
import mock
from base64 import b64encode

import pytest

import databricks_cli.stack.api as api
from requests.exceptions import HTTPError

TEST_STACK_PATH = 'stack/stack.json'
TEST_JOB_SETTINGS = {
    'name': 'my test job'
}
TEST_JOB_ALT_SETTINGS = {
    'name': 'my test job'
}
TEST_JOB_RESOURCE = {
    api.RESOURCE_ID: "job 1",
    api.RESOURCE_TYPE: "job",
    api.RESOURCE_PROPERTIES: TEST_JOB_SETTINGS
}
TEST_WORKSPACE_RESOURCE_ID = "notebook 1"
TEST_WORKSPACE_RESOURCE = {
    api.RESOURCE_ID: TEST_WORKSPACE_RESOURCE_ID,
    api.RESOURCE_TYPE: "workspace",
    api.RESOURCE_PROPERTIES: {
        "source_path": "dev/job1.py",
        "workspace_path": "/Users/example@example.com.com/dev/job",
        "object_type": "NOTEBOOK"
    }
}
TEST_DBFS_RESOURCE = {}
TEST_STACK = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_WORKSPACE_RESOURCE, TEST_JOB_RESOURCE, TEST_DBFS_RESOURCE]
}
TEST_STATUS = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_WORKSPACE_RESOURCE, TEST_JOB_RESOURCE, TEST_DBFS_RESOURCE],
    api.STACK_DEPLOYED: []
}

TEST_LANGUAGE = 'PYTHON'
TEST_FMT = 'SOURCE'


@pytest.fixture()
def stack_api():
    workspace_api_mock = mock.patch('databricks_cli.stack.api.WorkspaceApi')
    jobs_api_mock = mock.patch('databricks_cli.stack.api.JobsApi')
    dbfs_api_mock = mock.patch('databricks_cli.stack.api.DbfsApi')
    workspace_api_mock.return_value = mock.MagicMock()
    jobs_api_mock.return_value = mock.MagicMock()
    dbfs_api_mock.return_value = mock.MagicMock()
    stack_api = api.StackApi(mock.MagicMock())
    yield stack_api


class TestStackApi(object):
    def test_read_config(self, stack_api, tmpdir):
        """
            Test reading a stack configuration template
        """
        stack_path = os.path.join(tmpdir.strpath, TEST_STACK_PATH)
        os.makedirs(os.path.dirname(stack_path))
        with open(stack_path, "w+") as f:
            json.dump(TEST_STACK, f)
        config = stack_api.parse_config_file(stack_path)
        assert config == TEST_STACK

    def test_read_status(self, stack_api, tmpdir):
        """
            Test reading and parsing a deployed stack's status JSON file.
        """
        api.STACK_DIR = os.path.join(tmpdir.strpath, 'databricks', 'test')
        os.makedirs(api.STACK_DIR)
        status_path = os.path.join(api.STACK_DIR, 'test.json')
        with open(status_path, "w+") as f:
            json.dump(TEST_STATUS, f)

        status = stack_api.load_deploy_metadata('test')
        assert status == TEST_STATUS
        assert stack_api.deployed_resource_config == TEST_STATUS[api.STACK_RESOURCES]
        assert all(resource[api.RESOURCE_ID] in stack_api.deployed_resources
                   for resource in TEST_STATUS[api.STACK_DEPLOYED])

    def test_store_status(self, stack_api, tmpdir):

        assert True

    def test_download_paths(self, stack_api, tmpdir):
        """
            Test downloading of files to relative paths of the config template json file.
            - stack (directory)
              - stack.json (config file)
              - dev (directory)
                - a (workspace notebook)
                - b (dbfs init script)
        """
        stack_api.jobs_client = mock.MagicMock()
        stack_api.workspace_client = mock.MagicMock()
        assert True

    def test_deploy_paths(self, stack_api):
        stack_api.jobs_client = mock.MagicMock()
        stack_api.workspace_client = mock.MagicMock()
        assert True

    def test_deploy_job(self, stack_api):
        """
            Test Deploy Job Functionality
        """
        job_physical_id = 12345
        job_deploy_output = {'job_id': job_physical_id, 'job_settings': TEST_JOB_SETTINGS}
        stack_api.api_client.host = mock.MagicMock()
        stack_api.api_client.host.return_value = ""

        def _get_job(job_id):
            if job_id != job_physical_id:
                raise HTTPError()
            else:
                return job_deploy_output

        def _reset_job(data):
            if data['job_id'] != job_deploy_output['job_id']:
                raise Exception('Job Not Found')
            job_deploy_output['job_settings'] = data['new_settings']

        def _create_job(job_settings):
            job_deploy_output['job_settings'] = job_settings
            return {'job_id': job_physical_id}

        stack_api.jobs_client.create_job = mock.Mock(wraps=_create_job)
        stack_api.jobs_client.get_job = mock.Mock(wraps=_get_job)
        stack_api.jobs_client.reset_job = mock.Mock(wraps=_reset_job)

        # Deploy New job
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS)

        assert res_physical_id == job_physical_id
        assert res_deploy_output == job_deploy_output

        # Updating job
        job_deploy_output['job_settings'] = TEST_JOB_ALT_SETTINGS
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS,
                                                                  res_physical_id)
        assert res_deploy_output == job_deploy_output
        assert res_physical_id == job_physical_id

        # Try to update job that doesn't exist anymore
        job_physical_id = 123456
        job_deploy_output = {'job_id': job_physical_id, 'job_settings': TEST_JOB_SETTINGS}
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS,
                                                                  res_physical_id)
        assert res_deploy_output == job_deploy_output
        assert res_physical_id == job_physical_id

        assert stack_api.jobs_client.get_job.call_count == 5
        assert stack_api.jobs_client.reset_job.call_count == 1
        assert stack_api.jobs_client.create_job.call_count == 2

    def test_deploy_workspace(self, stack_api, tmpdir):
        """
            Test Deploy workspace functionality
        """

        filepath = os.path.join(tmpdir.strpath, 'dev', 'job.py')
        dirpath = os.path.join(tmpdir.strpath, 'directory')
        os.makedirs(os.path.dirname(filepath))
        with open(filepath, 'w+') as f:
            f.write('print("hi")')
        os.makedirs(dirpath)

        file_workspace_path = '/Users/jobs.py'
        dir_workspace_path = '/Users/directory'

        def _get_status_json(path):
            if path == file_workspace_path:
                return {}
            elif path == dir_workspace_path:
                return {}
            else:
                # Raise an error if the workspace path isn't correct
                raise Exception('Cant Find File')

        stack_api.api_client.host = mock.MagicMock()
        stack_api.api_client.host.return_value = ""
        stack_api.workspace_client.get_status_json = mock.Mock(wraps=_get_status_json)
        stack_api.workspace_client.import_workspace = mock.MagicMock()
        stack_api.workspace_client.import_workspace_dir = mock.MagicMock()
        stack_api.workspace_client.mkdirs = mock.MagicMock()

        file_properties = {'source_path': filepath, 'workspace_path': file_workspace_path}
        dir_properties = {'source_path': dirpath, 'workspace_path': dir_workspace_path}
        # Test property inference
        stack_api.deploy_workspace('file', file_properties, overwrite=False)
        assert stack_api.workspace_client.import_workspace.call_count == 1
        assert stack_api.workspace_client.import_workspace_dir.call_count == 0
        assert stack_api.workspace_client.mkdirs.call_count == 1
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == filepath
        assert stack_api.workspace_client.import_workspace.call_args[0][1] == file_workspace_path
        assert stack_api.workspace_client.import_workspace.call_args[0][2] == 'PYTHON'
        assert stack_api.workspace_client.import_workspace.call_args[0][3] == 'SOURCE'
        assert stack_api.workspace_client.import_workspace.call_args[0][4] is False

        stack_api.deploy_workspace('directory', dir_properties, overwrite=False)
        assert stack_api.workspace_client.import_workspace.call_count == 1
        assert stack_api.workspace_client.import_workspace_dir.call_count == 1
        assert stack_api.workspace_client.mkdirs.call_count == 1
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][0] == dirpath
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][1] == dir_workspace_path
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][2] is False

        # Test property inference
        file_properties['language'] = 'SCALA'
        file_properties['format'] = 'HTML'
        file_properties['object_type'] = 'NOTEBOOK'
        # Test property inference
        stack_api.deploy_workspace('file', file_properties, overwrite=True)
        assert stack_api.workspace_client.import_workspace.call_count == 2
        assert stack_api.workspace_client.import_workspace_dir.call_count == 1
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == filepath
        assert stack_api.workspace_client.import_workspace.call_args[0][1] == file_workspace_path
        assert stack_api.workspace_client.import_workspace.call_args[0][2] == 'SCALA'
        assert stack_api.workspace_client.import_workspace.call_args[0][3] == 'HTML'
        assert stack_api.workspace_client.import_workspace.call_args[0][4] is True

    def test_deploy_dbfs(self, stack_api, tmpdir):
        filepath = os.path.join(tmpdir.strpath, 'dev', 'job.sh')
        dirpath = os.path.join(tmpdir.strpath, 'directory')
        os.makedirs(os.path.dirname(filepath))
        with open(filepath, 'w+') as f:
            f.write('123')
        os.makedirs(dirpath)

        file_dbfs_path = 'dbfs:/tmp/jobs.sh'
        dir_dbfs_path = 'dbfs:/tmp/directory'

        def _get_status_json(dbfs_path):
            if dbfs_path.absolute_path == file_dbfs_path:
                return {}
            elif dbfs_path.absolute_path == dir_dbfs_path:
                return {}
            else:
                # Raise an error if the workspace path isn't correct
                raise Exception('Cant Find File')

        stack_api.dbfs_client.cp = mock.MagicMock()
        stack_api.api_client.host = mock.MagicMock()
        stack_api.api_client.host.return_value = ""
        stack_api.dbfs_client.get_status_json = mock.Mock(wraps=_get_status_json)

        file_properties = {'source_path': filepath, 'dbfs_path': file_dbfs_path}
        dir_properties = {'source_path': dirpath, 'dbfs_path': dir_dbfs_path}

        stack_api.deploy_dbfs('file', file_properties, overwrite=True)
        args, kwargs = stack_api.dbfs_client.cp.call_args
        assert kwargs['overwrite'] is True
        assert kwargs['src'] == filepath
        assert kwargs['dst'] == file_dbfs_path

        stack_api.deploy_dbfs('directory', dir_properties, overwrite=False)
        args, kwargs = stack_api.dbfs_client.cp.call_args
        assert kwargs['overwrite'] is False
        assert kwargs['src'] == dirpath
        assert kwargs['dst'] == dir_dbfs_path

        assert stack_api.dbfs_client.cp.call_count == 2

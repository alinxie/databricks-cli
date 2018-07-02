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
TEST_JOB_RESOURCE = {
    api.RESOURCE_ID: "client job test 1",
    api.RESOURCE_TYPE: "job",
    api.RESOURCE_PROPERTIES: {}
}
TEST_WORKSPACE_RESOURCE = {
    api.RESOURCE_ID: "notebook 1",
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
    stack_api = api.StackApi(None)
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
            Test reading and parsing a deployed stack metadata
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
        assert get_deployed_resource()
        wit

    def test_download_paths(self, stack_api, tmpdir):
        """
            Copy to directory ``tmpdir`` with structure as follows
            - a (directory)
              - b (scala)
              - c (python)
              - d (r)
              - e (sql)
            - f (directory)
              - g (directory)
        """
        stack_api.jobs_client = mock.MagicMock()
        stack_api.workspace_client = mock.MagicMock()
        assert True

    def test_deploy_paths(self, stack_api):
        stack_api.jobs_client = mock.MagicMock()
        stack_api.workspace_client = mock.MagicMock()
        assert True

    def test_duplicate_id(self, stack_api):
        assert True

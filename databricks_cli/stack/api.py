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

import os
import json
import traceback
from base64 import b64encode, b64decode
from datetime import datetime

import click

import _jsonnet
from requests.exceptions import HTTPError

from databricks_cli.dbfs.exceptions import LocalFileExistsException
from databricks_cli.sdk import WorkspaceService
from databricks_cli.jobs.api import JobsApi
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.version import version as CLI_VERSION
from databricks_cli.workspace.types import LanguageClickType, FormatClickType, WorkspaceFormat, \
    WorkspaceLanguage

DIRECTORY = 'DIRECTORY'
NOTEBOOK = 'NOTEBOOK'
LIBRARY = 'LIBRARY'
DEBUG_MODE = True
_home = os.path.expanduser('~')

# Resource Types
WORKSPACE_TYPE = 'workspace'
JOBS_TYPE = 'job'
DBFS_TYPE = 'dbfs'
CLUSTERS_TYPE = 'cluster'

# Config Outer Fields
STACK_NAME = 'name'
STACK_RESOURCES = 'resources'
STACK_DEPLOYED = 'deployed'

# Resource Fields
RESOURCE_ID = 'id'
RESOURCE_TYPE = 'type'
RESOURCE_PROPERTIES = 'properties'

# Deployed Resource Fields
RESOURCE_DEPLOY_INPUT = 'deploy_input'
RESOURCE_DEPLOY_OUTPUT = 'deploy_output'

class StackApi(object):
    def __init__(self, api_client):
        self.jobs_client = JobsApi(api_client)
        self.workspace_client = WorkspaceApi(api_client)
        self.api_client = api_client
        self.deployed_resources = {}
        self.deployed_config = {}

    def parse_config_file(self, filename, ext_vars={}):
        """Parse the jsonnet config, it also replace relative local path by actual path."""
        #if filename:
        if os.path.isdir(filename):
            if os.path.exists(os.path.join(filename, 'config.json')):
                filename = os.path.join(filename, 'config.json')

        parsed_conf = {}

        parsed_conf = json.loads(_jsonnet.evaluate_file(filename, ext_vars=ext_vars))
        local_dir = os.path.dirname(os.path.abspath(filename))
        os.chdir(local_dir)

        return parsed_conf

    def load_deploy_metadata(self, stack_name, stack_path=None):
        stack_file_path = os.path.join(_home, 'databricks', 'stacks', stack_name + '.json')
        parsed_conf = {}
        if os.path.exists(stack_file_path):
            with open(stack_file_path, 'r') as f:
                parsed_conf = json.load(f)

        if not parsed_conf:
            with open(stack_path, 'r') as f:
                parsed_conf = json.load(f)

        if 'resources' in parsed_conf:
            self.deployed_config= parsed_conf['resources']
        if 'deployed' in parsed_conf:
            self.deployed_resources = {resource[RESOURCE_ID]: resource for resource in parsed_conf['deployed']}

    def get_deployed_resource(self, resource_id, resource_type):
        if not self.deployed_resources:
            return {}, {}
        if resource_id in self.deployed_resources:
            deployed_resource = self.deployed_resources[resource_id]
            deployed_resource_type = deployed_resource['type']
            deployed_resource_input = deployed_resource['deploy_input']
            deployed_resource_output = deployed_resource['deploy_output']
            if resource_type != deployed_resource_type:
                click.echo("Resource %s is not of type %s", (resource_id, resource_type))
                return {}, {}
            return deployed_resource_input, deployed_resource_output
        return {}, {}

    def store_deploy_metadata(self, stack_name, data, custom_path=None):
        stack_dir = os.path.join(_home, 'databricks', 'stacks')
        if not os.path.exists(stack_dir):
            os.makedirs(stack_dir)
        stack_file_path = os.path.join(stack_dir, stack_name + ".json")
        with open(stack_file_path, 'w+') as f:
            click.echo('Storing deploy status metadata to %s' % stack_file_path)
            json.dump(data, f, indent=2)

        if custom_path:
            with open(custom_path, 'w+') as f:
                click.echo('Storing deploy status metadata to %s' % custom_path)
                json.dump(data, f, indent=2)

    def list_stacks(self):
        stack_dir = os.path.join(_home, 'databricks', 'stacks')
        if not os.path.exists(stack_dir):
            return []
        stack_files = os.listdir(stack_dir)
        return [filename.replace('.json', '') for filename in stack_files]

    def validate_source_path(self, source_path):
        return source_path


    def deploy_job(self, resource_id, job_settings, existing_deploy_input={}, existing_deploy_output={}):
        job_id = None
        print("Deploying job %s with settings: \n%s \n" % (resource_id, json.dumps(
            job_settings, indent=2, sort_keys=True, separators=(',', ': '))))

        if existing_deploy_input: # job exists
            if 'job_id' in existing_deploy_input:
                job_id = existing_deploy_input['job_id']

        if job_id:
            try:
                # Check if persisted job still exists, otherwise create new job.
                self.jobs_client.get_job(job_id)
            except HTTPError as e:
                job_id = None

        if job_id:
            click.echo("Updating Job: %s" % resource_id)
            self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})
        else:
            click.echo("Creating Job: %s" % resource_id)
            job_id = self.jobs_client.create_job(job_settings)['job_id']
            click.echo("%s Created with ID %s. Link: %s/#job/%s" % (resource_id, str(job_id), self.api_client.host, str(job_id)))

        deploy_input = {'job_id': job_id}
        deploy_output = self.jobs_client.get_job(job_id)

        return deploy_input, deploy_output

    def deploy_workspace(self, resource_id, resource_properties, existing_deploy_input={}, existing_deploy_output={}, overwrite=True):
        click.echo("Deploying workspace asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))
        local_path = self.validate_source_path(resource_properties['source_path'])
        workspace_path = resource_properties['workspace_path']

        lang_fmt = WorkspaceLanguage.to_language_and_format(local_path) # Guess language and format
        if lang_fmt:
            language, fmt = lang_fmt

        if 'language' in resource_properties:
            language = resource_properties['language']
        if 'format' in resource_properties:
            fmt = resource_properties['format']

        object_type = "DIRECTORY" if os.path.isdir(local_path) else "NOTEBOOK"
        if 'object_type' in resource_properties:
            object_type = resource_properties['object_type']

        click.echo('sync %s %s to %s' % (object_type, local_path, workspace_path))
        if object_type == 'NOTEBOOK':
            self.workspace_client.mkdirs(os.path.dirname(workspace_path)) # Make directory in workspace if not exist
            self.workspace_client.import_workspace(local_path, workspace_path, language, fmt, overwrite)
        elif object_type == 'DIRECTORY':
            self.workspace_client.import_workspace_dir(local_path, workspace_path, overwrite, exclude_hidden_files=True)

        deploy_input = {'path': workspace_path}
        deploy_output = self.workspace_client.get_status_json(workspace_path)

        return deploy_input, deploy_output

    def deploy_resource(self, resource, overwrite):
        resource_id, resource_type, deploy_input, deploy_output = None, None, None, None
        try:
            resource_id = resource[RESOURCE_ID]
            resource_type = resource[RESOURCE_TYPE]
            resource_properties = resource[RESOURCE_PROPERTIES]

            # Deployment
            deploy_input, deploy_output = self.get_deployed_resource(resource_id, resource_type)

            if resource_type == JOBS_TYPE:
                deploy_input, deploy_output = self.deploy_job(resource_id, resource_properties, deploy_input, deploy_output)
            elif resource_type == WORKSPACE_TYPE:
                deploy_input, deploy_output = self.deploy_workspace(resource_id, resource_properties, deploy_input, deploy_output, overwrite)
        except HTTPError as e:
            click.echo(click.style('Error: %s' % e.response.json(), 'red'))
        except KeyError as e:
            click.echo('Error in config template: Missing %s, skipping resource' % e)
        except Exception as e:
            if DEBUG_MODE:
                traceback.print_tb(e.__traceback__)
            click.echo(e)

        resource_deploy_info = {}
        resource_deploy_info[RESOURCE_ID] = resource_id
        resource_deploy_info[RESOURCE_TYPE] = resource_type
        resource_deploy_info['timestamp'] = datetime.now().timestamp()
        resource_deploy_info['deploy_input'] = deploy_input
        resource_deploy_info['deploy_output'] = deploy_output
        return resource_deploy_info

    def deploy(self, filename, overwrite, save_status_path):
        parsed_conf = self.parse_config_file(filename)
        stack_name = parsed_conf['name']
        self.load_deploy_metadata(stack_name)

        deploy_metadata = {}
        deploy_metadata['name'] = stack_name
        # deploy_metadata['version'] = parsed_conf['version']
        # deploy_metadata['cli_version'] = CLI_VERSION
        click.echo('Deploying stack %s' % stack_name)
        deploy_metadata['resources'] = parsed_conf['resources']
        deployed_resources = []
        for resource in parsed_conf['resources']:
            click.echo()
            click.echo("Deploying resource")
            deploy_status = self.deploy_resource(resource, overwrite)
            if deploy_status:
                deployed_resources.append(deploy_status)
        deploy_metadata['deployed'] = deployed_resources
        self.store_deploy_metadata(stack_name, deploy_metadata, save_status_path)

    def download_workspace(self, resource_id, resource_properties, deploy_input, deploy_output, overwrite):
        click.echo("Downloading workspace asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))
        local_path = self.validate_source_path(resource_properties['source_path'])
        workspace_path = resource_properties['workspace_path']

        if 'format' in resource_properties:
            fmt = resource_properties['format']
        else:
            fmt = WorkspaceFormat.SOURCE

        object_type = "DIRECTORY" if os.path.isdir(local_path) else "NOTEBOOK"
        if 'object_type' in resource_properties:
            object_type = resource_properties['object_type']

        click.echo('sync %s %s to %s' % (object_type, local_path, workspace_path))
        if object_type == 'NOTEBOOK':
            self.workspace_client.export_workspace(workspace_path, local_path, fmt, overwrite)
        elif object_type == 'DIRECTORY':
            self.workspace_client.export_workspace_dir(workspace_path, local_path, overwrite)

    def download_resource(self, resource, overwrite):
        resource_id = resource['id']
        resource_type = resource['type']
        resource_properties = resource['properties']

        # Deployment
        deploy_input, deploy_output = self.get_deployed_resource(resource_id, resource_type)

        try:
            if resource_type == WORKSPACE_TYPE:
                self.download_workspace(resource_id, resource_properties, deploy_input, deploy_output, overwrite)
        except HTTPError as e:
            click.echo("HTTP Error: \n %s" % (json.dumps(e.response)))
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            click.echo(str(e))

    def download(self, filename, overwrite):
        parsed_conf = self.parse_config_file(filename)
        #load_credentials(parsed_conf, ctx.obj['HC_CONTEXT'])
        for resource in parsed_conf['resources']:
            self.download_resource(resource, overwrite)

    def describe(self, stack_name):
        stored_deploy_metadata = self.load_deploy_metadata(stack_name)
        stored_deploy_metadata['jobs'] = self.get_job_info(stored_deploy_metadata['jobs'])
        stored_deploy_metadata['workspace'] = self.get_workspace_info(stored_deploy_metadata['workspace'])

        return stored_deploy_metadata

    def get_job_info(self, deployed_jobs):
        jobs = []
        for job in deployed_jobs:
            try:
                job_info = self.jobs_client.get_job(job['job_id'])
                job_url = '%s/#job/%s' % (self.api_client.host, str(job_info['job_id']))
                jobs.append({'job_name': job['job_name'], 'job_url': job_url, 'job_info': job_info})
            except HTTPError as e:
                jobs.append({'job_name': job['job_name'], 'job_url': "Job doesn't exist", 'job_info': e.response.json()})
        return jobs

    def get_workspace_info(self, workspace_paths):
        workspace_infos = []
        for workspace_path in workspace_paths:
            workspace_infos += self.workspace_client.list_objects(workspace_path)
        return workspace_infos




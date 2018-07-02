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
import traceback
from datetime import datetime
import time
import six

import click

from requests.exceptions import HTTPError

from databricks_cli.dbfs.exceptions import LocalFileExistsException
from databricks_cli.jobs.api import JobsApi
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.version import version as CLI_VERSION
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.workspace.types import WorkspaceFormat, WorkspaceLanguage

DEBUG_MODE = True
_home = os.path.expanduser('~')

# Stack Deployment Status Folder
STACK_DIR = os.path.join(_home, 'databricks', 'stacks', 'beta')

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
RESOURCE_PHYSICAL_ID = 'physical_id'
RESOURCE_DEPLOY_OUTPUT = 'deploy_output'


class StackApi(object):
    def __init__(self, api_client):
        self.jobs_client = JobsApi(api_client)
        self.workspace_client = WorkspaceApi(api_client)
        self.dbfs_client = DbfsApi(api_client)
        self.api_client = api_client
        self.deployed_resources = {}
        self.deployed_resource_config = {}

    def parse_config_file(self, filename):
        """Parse the json config"""
        parsed_conf = {}
        with open(filename, 'r') as f:
            parsed_conf = json.load(f)

        return parsed_conf

    def load_deploy_metadata(self, stack_name, stack_path=None):
        stack_file_path = os.path.join(STACK_DIR, stack_name + '.json')
        parsed_conf = {}
        if os.path.exists(stack_file_path):
            with open(stack_file_path, 'r') as f:
                parsed_conf = json.load(f)

        if not parsed_conf and stack_path:
            with open(stack_path, 'r') as f:
                parsed_conf = json.load(f)

        if STACK_RESOURCES in parsed_conf:
            self.deployed_resource_config = parsed_conf[STACK_RESOURCES]
        if STACK_DEPLOYED in parsed_conf:
            self.deployed_resources = {resource[RESOURCE_ID]: resource for resource in
                                       parsed_conf[STACK_DEPLOYED]}
        return parsed_conf

    def get_deployed_resource(self, resource_id, resource_type):
        """
        Returns the databricks physical ID of a resource with RESOURCE_ID and RESOURCE_TYPE

        :param resource_id: Internal stack identifier of resource
        :param resource_type: Resource type of stack resource
        :return: JSON object of Physical ID of resource on databricks
        """
        if not self.deployed_resources:
            return None
        if resource_id in self.deployed_resources:
            deployed_resource = self.deployed_resources[resource_id]
            deployed_resource_type = deployed_resource[RESOURCE_TYPE]
            deployed_physical_id = deployed_resource[RESOURCE_PHYSICAL_ID]
            if resource_type != deployed_resource_type:
                click.echo("Resource %s is not of type %s", (resource_id, resource_type))
                return None
            return deployed_physical_id
        return None

    def store_deploy_metadata(self, stack_name, data, custom_path=None):
        stack_file_path = os.path.join(STACK_DIR, stack_name + ".json")
        stack_file_folder = os.path.dirname(stack_file_path)
        if not os.path.exists(stack_file_folder):
            os.makedirs(stack_file_folder)
        with open(stack_file_path, 'w+') as f:
            click.echo('Storing deploy status metadata to %s' % stack_file_path)
            json.dump(data, f, indent=2)

        if custom_path:
            custom_path_folder = os.path.dirname(custom_path)
            if not os.path.exists(custom_path_folder):
                os.makedirs(custom_path_folder)
            with open(custom_path, 'w+') as f:
                click.echo('Storing deploy status metadata to %s' % os.path.abspath(custom_path))
                json.dump(data, f, indent=2)

    def validate_source_path(self, source_path):
        return os.path.abspath(source_path)

    def deploy_job(self, resource_id, job_settings, physical_id=None):
        job_id = None
        print("Deploying job %s with settings: \n%s \n" % (resource_id, json.dumps(
            job_settings, indent=2, sort_keys=True, separators=(',', ': '))))

        if physical_id:  # job exists
            if 'job_id' in physical_id:
                job_id = physical_id['job_id']

        if job_id:
            try:
                # Check if persisted job still exists, otherwise create new job.
                self.jobs_client.get_job(job_id)
            except HTTPError:
                job_id = None

        if job_id:
            click.echo("Updating Job: %s" % resource_id)
            click.echo("Link: %s/#job/%s" % (self.api_client.host, str(job_id)))
            self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})
        else:
            click.echo("Creating Job: %s" % resource_id)
            job_id = self.jobs_client.create_job(job_settings)['job_id']
            click.echo("%s Created with ID %s. Link: %s/#job/%s" % (
                resource_id, str(job_id), self.api_client.host, str(job_id)))

        physical_id = {'job_id': job_id}
        deploy_output = self.jobs_client.get_job(job_id)

        return physical_id, deploy_output

    def deploy_workspace(self, resource_id, resource_properties, physical_id=None, overwrite=False):
        click.echo("Deploying workspace asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))
        local_path = self.validate_source_path(resource_properties['source_path'])
        workspace_path = resource_properties['workspace_path']

        lang_fmt = WorkspaceLanguage.to_language_and_format(local_path)  # Guess language and format
        language, fmt = None, None
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
            self.workspace_client.mkdirs(
                os.path.dirname(workspace_path))  # Make directory in workspace if not exist
            self.workspace_client.import_workspace(local_path, workspace_path, language, fmt,
                                                   overwrite)
        elif object_type == 'DIRECTORY':
            self.workspace_client.import_workspace_dir(local_path, workspace_path, overwrite,
                                                       exclude_hidden_files=True)
        if physical_id and workspace_path != physical_id['path']:
            click.echo('Workspace asset %s had path changed from %s to %s' % (resource_id,
                                                                              physical_id['path'],
                                                                              workspace_path))
        physical_id = {'path': workspace_path}
        deploy_output = self.workspace_client.get_status_json(workspace_path)

        return physical_id, deploy_output

    def deploy_dbfs(self, resource_id, resource_properties, physical_id=None, overwrite=False):
        click.echo("Deploying dbfs asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))

        local_path = self.validate_source_path(resource_properties['source_path'])
        dbfs_path = resource_properties['dbfs_path']

        self.dbfs_client.cp(recursive=True, overwrite=overwrite, src=local_path, dst=dbfs_path)

        if physical_id and dbfs_path != physical_id['path']:
            click.echo('Workspace asset %s had path changed from %s to %s' % (resource_id,
                                                                              physical_id['path'],
                                                                              dbfs_path))
        physical_id = {'path': dbfs_path}
        deploy_output = self.dbfs_client.get_status_json(DbfsPath(dbfs_path))

        return physical_id, deploy_output

    def deploy_resource(self, resource, overwrite):
        resource_id, resource_type, physical_id, deploy_output = None, None, None, None
        success = True
        err_message = ""
        try:
            resource_id = resource[RESOURCE_ID]
            resource_type = resource[RESOURCE_TYPE]
            resource_properties = resource[RESOURCE_PROPERTIES]

            # Deployment
            physical_id = self.get_deployed_resource(resource_id, resource_type)

            if resource_type == JOBS_TYPE:
                physical_id, deploy_output = self.deploy_job(resource_id, resource_properties,
                                                             physical_id)
            elif resource_type == WORKSPACE_TYPE:
                physical_id, deploy_output = self.deploy_workspace(resource_id,
                                                                   resource_properties,
                                                                   physical_id, overwrite)
            elif resource_type == DBFS_TYPE:
                physical_id, deploy_output = self.deploy_dbfs(resource_id,
                                                              resource_properties,
                                                              physical_id, overwrite)
            else:
                click.echo("Resource type not found")
                return None
        except HTTPError as e:
            if DEBUG_MODE:
                traceback.print_tb(e.__traceback__)
            err_message = 'HTTP Error: %s' % e.response.json()
            click.echo(click.style(err_message, 'red'))
            success = False
        except KeyError as e:
            err_message = 'Error in config template for resource %s: Missing %s, skipping resource'\
                          % (resource_id, e)
            click.echo(click.style(err_message, 'red'))
            success = False
        except Exception as e:
            if DEBUG_MODE:
                traceback.print_tb(e.__traceback__)
            err_message = str(e)
            click.echo(click.style(err_message, 'red'))
            success = False

        resource_deploy_info = {RESOURCE_ID: resource_id, RESOURCE_TYPE: resource_type}
        if six.PY3:
            resource_deploy_info['timestamp'] = datetime.now().timestamp()
        elif six.PY2:
            resource_deploy_info['timestamp'] = time.mktime(datetime.now().timetuple())
        resource_deploy_info['physical_id'] = physical_id
        resource_deploy_info['deploy_output'] = deploy_output
        resource_deploy_info['success'] = success
        resource_deploy_info['error_message'] = err_message
        return resource_deploy_info

    def deploy(self, filename, overwrite, save_status_path=None):
        local_dir = os.path.dirname(os.path.abspath(filename))
        cli_cwd = os.getcwd()
        if save_status_path:
            save_status_path = os.path.abspath(save_status_path)
        os.chdir(local_dir)

        parsed_conf = self.parse_config_file(filename)
        stack_name = parsed_conf['name']

        self.load_deploy_metadata(stack_name, save_status_path)

        deploy_metadata = {'name': stack_name, 'cli_version': CLI_VERSION}
        # deploy_metadata['version'] = parsed_conf['version']
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
        os.chdir(cli_cwd)

    def download_workspace(self, resource_id, resource_properties, physical_id, overwrite):
        click.echo("Downloading workspace asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))
        local_path = self.validate_source_path(resource_properties['source_path'])
        workspace_path = resource_properties['workspace_path']

        if physical_id:
            if physical_id['workspace_path'] != workspace_path:
                click.echo("Change in workspace path from deployment")

        if 'format' in resource_properties:
            fmt = resource_properties['format']
        else:
            fmt = WorkspaceFormat.SOURCE

        object_type = "DIRECTORY" if os.path.isdir(local_path) else "NOTEBOOK"
        if 'object_type' in resource_properties:
            object_type = resource_properties['object_type']

        click.echo('sync %s %s to %s' % (object_type, local_path, workspace_path))
        if object_type == 'NOTEBOOK':
            try:
                self.workspace_client.export_workspace(workspace_path, local_path, fmt, overwrite)
            except LocalFileExistsException:
                click.echo('{} already exists locally as {}. Skip.'.format(workspace_path,
                                                                           local_path))
        elif object_type == 'DIRECTORY':
            self.workspace_client.export_workspace_dir(workspace_path, local_path, overwrite)

    def download_dbfs(self, resource_id, resource_properties, physical_id=None, overwrite=False):
        click.echo("Downloading dbfs asset %s with properties \n%s" % (resource_id, json.dumps(
            resource_properties, indent=2, sort_keys=True, separators=(',', ': '))))
        local_path = self.validate_source_path(resource_properties['source_path'])
        dbfs_path = resource_properties['dbfs_path']

        if physical_id:
            if physical_id['dbfs_path'] != dbfs_path:
                click.echo("Change in dbfs path from deployment")

        self.dbfs_client.cp(recursive=True, overwrite=overwrite, src=dbfs_path, dst=local_path)

        click.echo('sync %s to %s' % (local_path, dbfs_path))

    def download_resource(self, resource, overwrite):
        try:
            resource_id = resource[RESOURCE_ID]
            resource_type = resource[RESOURCE_TYPE]
            resource_properties = resource[RESOURCE_PROPERTIES]

            # Deployment
            physical_id = self.get_deployed_resource(resource_id, resource_type)
            click.echo()
            if resource_type == WORKSPACE_TYPE:
                self.download_workspace(resource_id, resource_properties, physical_id, overwrite)
            elif resource_type == DBFS_TYPE:
                self.download_dbfs(resource_id, resource_properties, physical_id, overwrite)
        except HTTPError as e:
            click.echo("HTTP Error: \n %s" % (json.dumps(e.response)))
        except KeyError as e:
            click.echo('Error in config template: Missing %s, skipping resource' % e)
        except Exception as e:
            if DEBUG_MODE:
                traceback.print_tb(e.__traceback__)
            click.echo(str(e))

    def download(self, filename, overwrite):
        local_dir = os.path.dirname(os.path.abspath(filename))
        cli_cwd = os.getcwd()
        parsed_conf = self.parse_config_file(local_dir)
        os.chdir(local_dir)
        for resource in parsed_conf['resources']:
            self.download_resource(resource, overwrite)
        os.chdir(cli_cwd)


    # WIP- describe stack
    def describe(self, stack_name):
        stored_deploy_metadata = self.load_deploy_metadata(stack_name)
        click.echo(json.dumps(stored_deploy_metadata, indent=2))
        return stored_deploy_metadata

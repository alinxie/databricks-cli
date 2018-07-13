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
from datetime import datetime
import time

import click

from databricks_cli.jobs.api import JobsApi

from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.workspace.types import WorkspaceLanguage
from databricks_cli.version import version as CLI_VERSION
from databricks_cli.stack.exceptions import StackError

MS_SEC = 1000
STACK_STATUS_INSERT = 'deployed'

# Resource Services
JOBS_SERVICE = 'jobs'
WORKSPACE_SERVICE = 'workspace'

# Config Outer Fields
STACK_NAME = 'name'
STACK_RESOURCES = 'resources'
STACK_DEPLOYED = 'deployed'

# Resource Fields
RESOURCE_ID = 'id'
RESOURCE_SERVICE = 'service'
RESOURCE_PROPERTIES = 'properties'

# Deployed Resource Fields
RESOURCE_PHYSICAL_ID = 'physical_id'
RESOURCE_DEPLOY_OUTPUT = 'deploy_output'
RESOURCE_DEPLOY_TIMESTAMP = 'timestamp'
CLI_VERSION_KEY = 'cli_version'


class StackApi(object):
    def __init__(self, api_client):
        self.jobs_client = JobsApi(api_client)
        self.workspace_client = WorkspaceApi(api_client)

    def _load_json(self, path):
        """
        Parse a json file to a readable dict format.
        Returns an empty dictionary if the path doesn't exist.

        :param path: File path of the JSON stack configuration template.
        :return: dict of parsed JSON stack config template.
        """
        stack_conf = {}
        if os.path.exists(path):
            with open(path, 'r') as f:
                stack_conf = json.load(f)

        return stack_conf

    def _json_type_handler(self, obj):
        """
        Helper function to convert certain objects into a compatible JSON format.

        Right now, converts a datetime object to an integer timestamp.

        :param obj: Object that may be a datetime object.
        :return: Timestamp integer if object is a datetime object.
        """
        if isinstance(obj, datetime):
            # Get timestamp of datetime object- works with python2 and 3
            return int(time.mktime(obj.timetuple()))
        raise TypeError("Object of type '%s' is not JSON serializable" % type(obj))

    def _save_json(self, path, data):
        """
        Writes data to a JSON file.

        :param path: Path of JSON file.
        :param data: dict- data that wants to by written to JSON file
        :return: None
        """
        with open(path, 'w+') as f:
            json.dump(data, f, indent=2, sort_keys=True, default=self._json_type_handler)

    def _generate_stack_status_path(self, stack_path):
        """
        Given a path to the stack configuration template JSON file, generates a path to where the
        deployment status JSON will be stored after successful deployment of the stack.

        :param stack_path: Path to the stack config template JSON file
        :return: The path to the stack status file.

        >>> self._generate_stack_status_path('./stack.json')
        './stack.deployed.json'
        """
        stack_path_split = stack_path.split('.')
        stack_path_split.insert(-1, STACK_STATUS_INSERT)
        return '.'.join(stack_path_split)

    def _get_resource_to_status_map(self, stack_status):
        """
        Returns a dictionary that maps a resource's (id, service) to the resource's status
        from the last deployment

        The key for this dictionary is the resource's (id, service) so that we don't load
        persisted resources with the wrong resource service.
        """
        return {(resource_status[RESOURCE_ID], resource_status[RESOURCE_SERVICE]): resource_status
                for resource_status in stack_status[STACK_DEPLOYED]}

    def put_job(self, job_settings):
        """
        Given settings of the job in job_settings, create a new job. For purposes of idempotency
        and to reduce leaked resources in alpha versions of stack deployment, if a job exists
        with the same name, that job will be updated. If multiple jobs are found with the same name,
        the deployment will abort.

        :param job_settings:
        :return: job_id, Physical ID of job on Databricks server.
        """
        if 'name' not in job_settings:
            raise StackError("Please supply 'name' in job resource 'resource_properties'")
        job_name = job_settings['name']
        jobs_same_name = self.jobs_client._list_jobs_by_name(job_name)
        if len(jobs_same_name) > 1:
            raise StackError("Multiple jobs with the same name '%s' already exist, aborting"
                             " stack deployment" % job_name)
        elif len(jobs_same_name) == 1:
            existing_job = jobs_same_name[0]
            creator_name = existing_job['creator_user_name']
            timestamp = existing_job['created_time'] / MS_SEC  # Convert to readable date.
            date_created = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            click.echo("Warning: Job exists with same name '%s' created by %s on %s. Job will "
                       "be overwritten" % (job_name, creator_name, date_created))
            self.update_job(job_settings, existing_job['job_id'])
            return existing_job['job_id']
        else:
            click.echo("Creating new job")
            job_id = self.jobs_client.create_job(job_settings)['job_id']
            return job_id

    def update_job(self, job_settings, job_id):
        """
        Given job settings and an existing job_id of a job, update the job settings on databricks.

        :param job_settings: job settings to update the job with.
        :param job_id: physical job_id of job in databricks server.
        """
        click.echo("Updating Job")
        self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})

    def deploy_job(self, resource_properties, physical_id=None):
        """
        Deploys a job resource by either creating a job if the job isn't kept track of through
        the physical_id of the job or updating an existing job. The job is created or updated using
        the the settings specified in the inputted job_settings.

        :param resource_properties: A dict of the Databricks JobSettings data structure
        :param physical_id: A dict object containing 'job_id' field of job identifier in Databricks
        server

        :return: tuple of (physical_id, deploy_output), where physical_id contains a 'job_id' field
        of the physical job_id of the job on databricks. deploy_output is the output of the job
        from databricks when a GET request is called for it.
        """
        job_settings = resource_properties  # resource_properties of jobs are solely job settings.

        if physical_id:
            job_id = physical_id['job_id']
            self.update_job(job_settings, physical_id['job_id'])
        else:
            job_id = self.put_job(job_settings)
        click.echo("Job deployed on Databricks with job_id %s" % job_id)
        physical_id = {'job_id': job_id}
        deploy_output = self.jobs_client.get_job(job_id)
        return physical_id, deploy_output

    def deploy_workspace(self, resource_properties, physical_id, overwrite):
        """
        Deploy workspace asset.
        :param resource_properties:
        :param physical_id:
        :param overwrite:
        :return:
        """
        try:
            local_path = resource_properties['source_path']
            workspace_path = resource_properties['path']
        except KeyError as e:
            raise StackError("%s doesn't exist in workspace resource properties" % str(e))

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

        if physical_id and physical_id['path'] != workspace_path:
            click.echo("Workspace asset had path changed from %s to %s" % (physical_id['path'],
                                                                           workspace_path))
        deploy_output = self.workspace_client.get_status_json(workspace_path)

        return {'path': workspace_path}, deploy_output

    def deploy_resource(self, resource_config, resource_status=None, overwrite=False):
        """
        Deploys a resource given a resource information extracted from the stack JSON configuration
        template.

        :param resource_config: A dict of the resource with fields of RESOURCE_ID, RESOURCE_SERVICE
        and RESOURCE_PROPERTIES.
        ex. {'id': 'example-resource', 'service': 'jobs', 'properties': {...}}
        :param resource_status: A dict of the resource's deployment info from the last
        deployment. Will be None if this is the first deployment.
        ex. {'id': 'example-resource', 'service': 'jobs', 'physical_id': {...}}
        :param overwrite: Whether or not to overwrite Workspace files.
        :return: dict resource_status- A dictionary of deployment information of the
        resource to be stored at deploy time. It includes the resource id of the resource along
        with the physical id and deploy output of the resource.
        ex. {'id': 'example-resource', 'service': 'jobs', 'physical_id': {'job_id': 123},
        'timestamp': 123456789, 'deploy_output': {..}}
        """
        resource_id = resource_config[RESOURCE_ID]
        resource_service = resource_config[RESOURCE_SERVICE]
        resource_properties = resource_config[RESOURCE_PROPERTIES]
        physical_id = resource_status[RESOURCE_PHYSICAL_ID] if resource_status else None

        if resource_service == JOBS_SERVICE:
            click.echo("Deploying job '%s' with properties: \n%s \n" % (resource_id, json.dumps(
                resource_properties, indent=2, separators=(',', ': '))), nl=False)
            physical_id, deploy_output = self.deploy_job(resource_properties,
                                                         physical_id)
        elif resource_service == WORKSPACE_SERVICE:
            click.echo(
                "Deploying workspace asset %s with properties \n%s" % (resource_id, json.dumps(
                    resource_properties, indent=2, separators=(',', ': '))))
            physical_id, deploy_output = self.deploy_workspace(resource_properties,
                                                               physical_id, overwrite)
        else:
            raise StackError("Resource service '%s' not supported" % resource_service)

        new_resource_status = {RESOURCE_ID: resource_id, RESOURCE_SERVICE: resource_service,
                               RESOURCE_DEPLOY_TIMESTAMP: datetime.now(),
                               RESOURCE_PHYSICAL_ID: physical_id,
                               RESOURCE_DEPLOY_OUTPUT: deploy_output}
        return new_resource_status

    def validate_status(self, stack_status):
        """
        Validate fields within a stack status. This ensures that a stack status has the
        necessary fields for stack deployment to function well.

        If there is an error here, then it is either an implementation error that must be fixed by
        a developer or the User edited the stack status file created by the program.

        :param stack_status: dict- stack status that is created by the program.
        :return: None. Raises errors to stop deployment if there is a problem.
        """
        if STACK_NAME not in stack_status:
            raise StackError("'%s' not in status" % STACK_NAME)
        if STACK_RESOURCES not in stack_status:
            raise StackError("'%s' not in status" % STACK_RESOURCES)
        if STACK_DEPLOYED not in stack_status:
            raise StackError("'%s' not in status" % STACK_DEPLOYED)
        for deployed_resource in stack_status[STACK_DEPLOYED]:
            if RESOURCE_ID not in deployed_resource:
                raise StackError("%s doesn't exist in deployed resource status" % RESOURCE_ID)
            if RESOURCE_SERVICE not in deployed_resource:
                raise StackError("%s doesn't exist in deployed resource status" % RESOURCE_SERVICE)
            if RESOURCE_PHYSICAL_ID not in deployed_resource:
                raise StackError("%s doesn't exist in deployed resource status" %
                                 RESOURCE_PHYSICAL_ID)

    def validate_config(self, stack_config):
        """
        Validate fields within a stack configuration. This ensures that an inputted configuration
        has the necessary fields for stack deployment to function well.

        :param stack_config: dict- stack config that is inputted by the user.
        :return: None. Raises errors to stop deployment if there is a problem.
        """
        if STACK_NAME not in stack_config:
            raise StackError("'%s' not in configuration" % STACK_NAME)
        if STACK_RESOURCES not in stack_config:
            raise StackError("'%s' not in configuration" % STACK_RESOURCES)
        for resource in stack_config[STACK_RESOURCES]:
            if RESOURCE_ID not in resource:
                raise StackError("%s doesn't exist in resource config" % RESOURCE_ID)
            if RESOURCE_SERVICE not in resource:
                raise StackError("%s doesn't exist in resource config" % RESOURCE_SERVICE)
            if RESOURCE_PROPERTIES not in resource:
                raise StackError("%s doesn't exist in resource config" % RESOURCE_PROPERTIES)

    def deploy_config(self, stack_config, stack_status=None, overwrite=False):
        self.validate_config(stack_config)
        if stack_status:
            self.validate_status(stack_status)
            resource_id_to_status = self._get_resource_to_status_map(stack_status)
        else:
            resource_id_to_status = {}

        stack_name = stack_config[STACK_NAME]
        click.echo('Deploying stack %s' % stack_name)
        resource_statuses = []

        for resource_config in stack_config[STACK_RESOURCES]:
            click.echo()
            click.echo("Deploying resource")
            # Retrieve resource deployment info from the last deployment.
            resource_map_key = (resource_config[RESOURCE_ID], resource_config[RESOURCE_SERVICE])
            resource_status = resource_id_to_status[resource_map_key] \
                if resource_map_key in resource_id_to_status else None
            # Deploy resource, get resource_status
            new_resource_status = self.deploy_resource(resource_config, resource_status, overwrite)
            resource_statuses.append(new_resource_status)

        # stack deploy status is original config with deployed resource statuses added
        new_stack_status = stack_config
        new_stack_status.update({STACK_DEPLOYED: resource_statuses})
        new_stack_status.update({CLI_VERSION_KEY: CLI_VERSION})

        # Validate that the status has been created correctly
        self.validate_status(new_stack_status)

        return new_stack_status

    def deploy(self, config_path, overwrite):
        """
        Deploys a stack given stack JSON configuration template at path config_path.

        Loads the JSON template as well as status JSON if stack has been deployed before.
        After going through each of the resources and deploying them, stores status JSON
        of deployment with deploy status of each resource deployment.

        :param config_path: Path to stack JSON configuration template. Must have the fields of
        'name', the name of the stack and 'resources', a list of stack resources.
        :param overwrite: boolean- whether or not to overwrite existing files or
        :return: None.
        """
        config_dir = os.path.dirname(os.path.abspath(config_path))
        cli_cwd = os.getcwd()
        os.chdir(config_dir)  # Switch current working directory to where json config is stored
        try:
            stack_config = self._load_json(config_path)
            status_path = self._generate_stack_status_path(config_path)
            stack_status = self._load_json(status_path)
            new_stack_status = self.deploy_config(stack_config, stack_status, overwrite)

            self._save_json(status_path, new_stack_status)
            os.chdir(cli_cwd)
        except Exception:
            # For any exception during deployment, set cwd back to what it was.
            os.chdir(cli_cwd)
            raise

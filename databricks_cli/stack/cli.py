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
import click
import json
from tabulate import tabulate
from requests.exceptions import HTTPError

from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.dbfs.exceptions import LocalFileExistsException
from databricks_cli.stack.api import StackApi
from databricks_cli.workspace.types import LanguageClickType, FormatClickType, WorkspaceFormat, \
    WorkspaceLanguage


def get_filename_from_globs(globs):
    filenames = []
    globs = globs.split(',')
    for glob_pattern in globs:
        filenames_from_glob = [f for f in glob.glob(glob_pattern) if f.endswith('.json')]
        print('For glob %s, found file(s) %s' % (glob_pattern, ', '.join(filenames_from_glob)))
        filenames.extend(filenames_from_glob)
    return filenames


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploy stack given a JSON configuration of the stack')
@click.option('--filenames', '-f', help='Comma separated json files.')
@click.option('--overwrite', '-o', is_flag=True, help='If overwrite existing notebooks in the workspace.')
@click.option('--save-status', '-s', help='Path to save deploy status JSON file at.')
@profile_option
@eat_exceptions
@provide_api_client
def deploy(api_client, filenames, overwrite, save_status):
    """
    Deploy a stack to the databricks workspace given a JSON stack configuration.
    """
    # if filenames is None and globs is None:
    #     raise Exception('Neither filenames nor globs is specified')

    # if filenames is not None and globs is not None:
    #     raise Exception(
    #         'Both of filenames and globs are specified! Please only specify one of them.')

    if filenames is not None:
        filenames = filenames.split(',')
    # else:
    #     filenames = get_filename_from_globs(globs)

    for filename in filenames:
        print('Deploying stack in: ' + filename)

        StackApi(api_client).deploy(filename, overwrite, save_status)
        print('#' * 80 + '\n')


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Download the associated resources associated with a databricks resource stack.')
@click.option('--filename', '-f', type=click.Path(exists=True), required=True, help='Filename of the json config.')
@click.option('--overwrite', '-o', is_flag=True, help='If overwrite the existing notebook.')
@profile_option
@eat_exceptions
@provide_api_client
def download(api_client, filename, overwrite):
    """
    Sync a local folder from workspace. It reads a json config file to determine the local path
    and remote path.
    """

    StackApi(api_client).download(filename, overwrite)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Describe a deployed stack of resources')
@click.option('--stack-name', '-s', required=True, help='Stack Name.')
@profile_option
@eat_exceptions
@provide_api_client
def describe(api_client, stack_name):
    """
    Describe a deployed stack of resources.
    """

    stack_description = StackApi(api_client).describe(stack_name)
    click.echo("STACK NAME: %s" % stack_description['name'])
    click.echo("STACK VERSION: %s" % stack_description['version'])
    click.echo("CLI VERSION: %s" % stack_description['cli_version'])
    click.echo()
    click.echo("WORKSPACE:")
    workspace_table = tabulate(
        [obj.to_row(is_long_form=True, is_absolute=True) for obj in stack_description['workspace']],
        tablefmt='plain')
    click.echo(workspace_table)
    click.echo()
    click.echo("JOBS:")
    for job in stack_description['jobs']:
        click.echo("Job Name: %s" % job['job_name'])
        click.echo(click.style(job['job_url'], fg='green'))
        click.echo(json.dumps(job['job_info'], indent=2))
        click.echo()


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='list currently deployed stacks')
@profile_option
@eat_exceptions
@provide_api_client
def stack_list(api_client):
    """
    List currently deployed stacks.
    """
    stacks = StackApi(api_client).list_stacks()
    for stack in stacks:
        click.echo(stack)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to deploy and download Databricks resource stacks.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@profile_option
def stack_group():
    """
    Utility to deploy and download Databricks resource stacks.
    """
    pass


stack_group.add_command(deploy, name='deploy')
stack_group.add_command(download, name='download')
# stack_group.add_command(describe, name='describe')
# stack_group.add_command(stack_list, name='list')

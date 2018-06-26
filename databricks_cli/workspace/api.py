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
from base64 import b64encode, b64decode

import click
from requests.exceptions import HTTPError

from databricks_cli.dbfs.exceptions import LocalFileExistsException
from databricks_cli.sdk import WorkspaceService
<<<<<<< HEAD
from databricks_cli.workspace.types import WorkspaceFormat, WorkspaceLanguage
=======
from databricks_cli.workspace.types import LanguageClickType, FormatClickType, WorkspaceFormat, \
    WorkspaceLanguage
>>>>>>> Some changes ish

DIRECTORY = 'DIRECTORY'
NOTEBOOK = 'NOTEBOOK'
LIBRARY = 'LIBRARY'


class WorkspaceFileInfo(object):
    def __init__(self, path, object_type, language=None):
        self.path = path
        self.object_type = object_type
        self.language = language

    def to_row(self, is_long_form, is_absolute):
        path = self.path if is_absolute else self.basename
        if self.is_dir:
            stylized_path = click.style(path, 'cyan')
        elif self.is_library:
            stylized_path = click.style(path, 'green')
        else:
            stylized_path = path
        if is_long_form:
            return [self.object_type, stylized_path, self.language]
        else:
            return [stylized_path]

    @property
    def is_dir(self):
        return self.object_type == DIRECTORY

    @property
    def is_notebook(self):
        return self.object_type == NOTEBOOK

    @property
    def is_library(self):
        return self.object_type == LIBRARY

    @property
    def basename(self):
        return os.path.basename(self.path)

    @classmethod
    def from_json(cls, deserialized_json):
        return cls(**deserialized_json)


class WorkspaceApi(object):
    def __init__(self, api_client):
        self.client = WorkspaceService(api_client)

    def get_status(self, workspace_path):
        return WorkspaceFileInfo.from_json(self.client.get_status(workspace_path))
    
    def get_status_json(self, workspace_path):
        return self.client.get_status(workspace_path)

    def list_objects(self, workspace_path):
        response = self.client.list(workspace_path)
        # This case is necessary when we list an empty dir in the workspace.
        # TODO(andrewmchen): We should make our API respond with a json with 'objects' field even
        # in this case.
        if 'objects' not in response:
            return []
        objects = response['objects']
        return [WorkspaceFileInfo.from_json(f) for f in objects]

    def mkdirs(self, workspace_path):
        self.client.mkdirs(workspace_path)

    def import_workspace(self, source_path, target_path, language, fmt, is_overwrite):
        with open(source_path, 'rb') as f:
            # import_workspace must take content that is typed str.
            content = b64encode(f.read()).decode()
            self.client.import_workspace(
                target_path,
                fmt,
                language,
                content,
                is_overwrite)

    def export_workspace(self, source_path, target_path, fmt, is_overwrite):
        """
        Faithfully exports the source_path to the target_path. Does not
        attempt to do any munging of the target_path if it is a directory.
        """
        if os.path.exists(target_path) and not is_overwrite:
            raise LocalFileExistsException('Target {} already exists.'.format(target_path))
        output = self.client.export_workspace(source_path, fmt)
        content = output['content']
        # Will overwrite target_path.
        with open(target_path, 'wb') as f:
            decoded = b64decode(content)
            f.write(decoded)

    def delete(self, workspace_path, is_recursive):
        self.client.delete(workspace_path, is_recursive)

    def import_workspace_dir(self, source_path, target_path, overwrite, exclude_hidden_files):
        filenames = os.listdir(source_path)
        if exclude_hidden_files:
            # for now, just exclude hidden files or directories based on starting '.'
            filenames = [f for f in filenames if not f.startswith('.')]
        try:
            self.mkdirs(target_path)
        except HTTPError as e:
            click.echo(e.response.json())
            return
        for filename in filenames:
            cur_src = os.path.join(source_path, filename)
            # don't use os.path.join here since it will set \ on Windows
            cur_dst = target_path.rstrip('/') + '/' + filename
            if os.path.isdir(cur_src):
                self.import_workspace_dir(cur_src, cur_dst, overwrite, exclude_hidden_files)
            elif os.path.isfile(cur_src):
                ext = WorkspaceLanguage.get_extension(cur_src)
                if ext != '':
                    cur_dst = cur_dst[:-len(ext)]
                    (language, file_format) = WorkspaceLanguage.to_language_and_format(cur_src)
                    self.import_workspace(cur_src, cur_dst, language, file_format, overwrite)
                    click.echo('{} -> {}'.format(cur_src, cur_dst))
                else:
                    extensions = ', '.join(WorkspaceLanguage.EXTENSIONS)
                    click.echo(('{} does not have a valid extension of {}. Skip this file and ' +
                                'continue.').format(cur_src, extensions))

    def export_workspace_dir(self, source_path, target_path, overwrite):
<<<<<<< HEAD
=======
        assert self.get_status(source_path).is_dir, 'The source path must be a directory. {}' \
        .format(source_path)
        
>>>>>>> Some changes ish
        if os.path.isfile(target_path):
            click.echo('{} exists as a file. Skipping this subtree {}'
                       .format(target_path, source_path))
            return
        if not os.path.isdir(target_path):
            os.makedirs(target_path)
        for obj in self.list_objects(source_path):
            cur_src = obj.path
            cur_dst = os.path.join(target_path, obj.basename)
            if obj.is_dir:
                self.export_workspace_dir(cur_src, cur_dst, overwrite)
            elif obj.is_notebook:
                cur_dst = cur_dst + WorkspaceLanguage.to_extension(obj.language)
                try:
                    self.export_workspace(cur_src, cur_dst, WorkspaceFormat.SOURCE, overwrite)
                    click.echo('{} -> {}'.format(cur_src, cur_dst))
                except LocalFileExistsException:
                    click.echo('{} already exists locally as {}. Skip.'.format(cur_src, cur_dst))
            else:
                click.echo('{} is neither a dir or a notebook. Skip.'.format(cur_src))

Stack Configuration Template JSON Schema
========================================

Outer Fields
------------
``"name"``: REQUIRED- The name of the stack. When the stack deployment status is persisted, it will take the
name of <name>.json

``"resources"``: REQUIRED-  A list of stack resources. The specification of the resource fields is in the next section.

Resource Fields
---------------
``"id"``: REQUIRED- This is a unique stack identifier of the resource that the stack will use

``"service"``: ``"jobs|workspace|dbfs"``- REQUIRED- The databricks service a resource is associated with.

``"properties"``: REQUIRED- This is a JSON object of properties related to the resource and is different
depending on the type of resource

Resource Service "properties"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
| ``"service"``    | ``"properties"`` from REST API used in Stack CLI                                                                                                                                                                                                                                                                      | ``"properties"`` only used in Stack CLI                                                                     |
+==================+=======================================================================================================================================================================================================================================================================================================================+=============================================================================================================+
| ``"workspace"``  | ``"path"``: REQUIRED- Matching remote Workspace paths of notebooks or directories.                                                                                                                                                                                                                                    | ``"source_path"``: REQUIRED- Local source path of Workspace notebooks or directories.                       |
|                  |                                                                                                                                                                                                                                                                                                                       |                                                                                                             |
|                  | ``"object_type"``: ``"NOTEBOOK|DIRECTORY"`` REQUIRED- This specifies the whether a notebook or directory is being managed by the stack. This corresponds with the `ObjectType <https://docs.databricks.com/api/latest/workspace.html#objecttype>`_ REST API data structure.                                           |                                                                                                             |
|                  |                                                                                                                                                                                                                                                                                                                       |                                                                                                             |
|                  | ``"language"``: ``"SCALA|PYTHON|SQL|R"`` OPTIONAL- This is the language of the notebook and should only be specified if ``"object_type=="NOTEBOOK"``. This corresponds with the Databricks `Language <https://docs.databricks.com/api/latest/workspace.html#language>`_                                               |                                                                                                             |
|                  | REST API data structure. If not provided, the language will be inferred from the file extension.                                                                                                                                                                                                                      |                                                                                                             |
|                  |                                                                                                                                                                                                                                                                                                                       |                                                                                                             |
|                  | ``"format"``: ``"SOURCE|DBC|HTML|IPYNB"`` OPTIONAL- This is the export format of the notebook. This corresponds with the Databricks `ExportFormat <https://docs.databricks.com/api/latest/workspace.html#exportformat>`_ REST API data structure. If not provided, will default to ``"SOURCE"``.                      |                                                                                                             |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
| ``"jobs"``       | Any setting in `JobSettings <https://docs.databricks.com/api/latest/jobs.html#jobsettings>`_ REST API data structure. The following two are required:                                                                                                                                                                 | None                                                                                                        |
|                  |                                                                                                                                                                                                                                                                                                                       |                                                                                                             |
|                  | ``"existing_cluster_id"`` OR ``"new_cluster"``: REQUIRED- Either `NewCluster <https://docs.databricks.com/api/latest/jobs.html#jobsettings>`_ JSON of a new cluster or string of cluster_id of an existing cluster                                                                                                    |                                                                                                             |
|                  |                                                                                                                                                                                                                                                                                                                       |                                                                                                             |
|                  | ``"name"``: REQUIRED- Name of the job to be deployed. In the REST API this is not required, but for purposes of not creating too many duplicate jobs, we are enforcing unique names in stack deployed jobs                                                                                                            |                                                                                                             |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
| ``"dbfs"``       | ``"path"``: REQUIRED- Matching remote DBFS path. MUST start with ``dbfs:/`` (ex. ``dbfs:/this/is/a/sample/path``)                                                                                                                                                                                                     | ``"source_path"``: REQUIRED- Local source path of DBFS files or directories.                                |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+

Example Config Template
-----------------------

.. code::

    {
      "name": "test_stack",
      "resources": [
        {
          "id": "job1",
          "service": "workspace",
          "properties": {
            "source_path": "dev/job1.py",
            "path": "/Users/example@example.com/dev/job1",
            "language": "PYTHON",
            "format": "SOURCE",
            "object_type": "NOTEBOOK"
          }
        },
        {
          "id": "example directory",
          "service": "workspace",
          "properties": {
            "source_path": "prod",
            "path": "/Users/example@example.com/example_dir",
            "object_type": "DIRECTORY"
          }
        },
        {
          "id": "job1 in dbfs",
          "service": "dbfs",
          "properties": {
            "source_path": "dev/job1.py",
            "path": "dbfs:/example_dbfs_dir/job1.py"
          }
        },
        {
          "id": "client job test 1",
          "service": "jobs",
          "properties": {
            "name": "Client job test 1",
            "new_cluster": {
              "spark_version": "4.0.x-scala2.11",
              "node_type_id": "r3.xlarge",
              "aws_attributes": {
                "availability": "SPOT"
              },
              "num_workers": 3
            },
            "timeout_seconds": 7200,
            "max_retries": 1,
            "schedule": {
              "quartz_cron_expression": "0 15 22 ? * *",
              "timezone_id": "America/Los_Angeles"
            },
            "notebook_task": {
              "notebook_path": "/Users/example@example.com/job1"
            }
          }
        },
        {
          "id": "client job test 2",
          "service": "jobs",
          "properties": {
            "name": "client job test 2",
            "new_cluster": {
              "spark_version": "4.0.x-scala2.11",
              "node_type_id": "r3.xlarge",
              "aws_attributes": {
                "availability": "SPOT"
              },
              "num_workers": 1
            },
            "timeout_seconds": 1200,
            "max_retries": 2,
            "notebook_task": {
              "notebook_path": "/Users/example@example.com/example_dir/prod/common/prodJob"
            }
          }
        }
      ]
    }

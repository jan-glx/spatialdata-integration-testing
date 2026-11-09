"""The two pipelines, each one DAG built from the shared task builders (airflow_tasks.py).

They used to be DAGs that triggered the per-step DAGs (config_update_repos, tests_all, ...) and
waited for them. That made every step a separate DAG with its own pause state: a run triggered
on a paused DAG stays queued forever, and the orchestrator waited on it without a timeout. As
tasks of one DAG, the steps can't hang that way, and the graph shows the whole pipeline.

The per-step DAGs still exist for running a single step by hand.
"""
from airflow import DAG
from airflow.sdk import TaskGroup
from spatialdata_data_converter.config import get_airflow_default_args
from spatialdata_data_converter.airflow_tasks import (
    REPOSITORY_PARAMS,
    build_create_symlinks_task,
    build_download_spatialdata_io_datasets_task,
    build_download_task,
    build_install_env_task,
    build_notebook_docs_task,
    build_tests_task,
    build_to_zarr_task,
    build_update_repos_task,
    update_repos_env,
)

default_args = get_airflow_default_args()

# --------- First Run Orchestrator ---------
# update repos -> create symlinks -> install env -> download all data
orchestrator_first_run = DAG(
    '0_orchestrator_first_run',
    default_args=default_args,
    description='First run: setup repos, symlinks, env, then download all data',
    schedule=None,
    catchup=False,
    max_active_runs=1,  # runs share one workspace and one ome_sdc env: a second run waits (queued)
    is_paused_upon_creation=False,
)

with orchestrator_first_run:
    first_run_update_repos = build_update_repos_task(dag=orchestrator_first_run)
    first_run_create_symlinks = build_create_symlinks_task(dag=orchestrator_first_run)
    first_run_install_env = build_install_env_task(dag=orchestrator_first_run)
    with TaskGroup('download') as first_run_download:
        build_download_task(dag=orchestrator_first_run)
        build_download_spatialdata_io_datasets_task(dag=orchestrator_first_run)

    first_run_update_repos >> first_run_create_symlinks >> first_run_install_env >> first_run_download

# --------- Test Workflow Orchestrator ---------
# update repos -> install env -> tests -> convert to zarr -> docs notebooks
orchestrator_test_workflow = DAG(
    '0_orchestrator_test_workflow',
    default_args=default_args,
    description='Test workflow: update repos, env, run tests, convert to zarr, run notebooks',
    schedule='0 0 * * *',  # Every day at midnight
    catchup=False,
    max_active_runs=1,  # runs share one workspace and one ome_sdc env: a second run waits (queued)
    # Paused on a fresh instance: the nightly run is switched on deliberately per instance
    # (dev and prod running it at the same time is what pushed manny into OOM on 3 Nov 2026).
    is_paused_upon_creation=True,
    # Per-run branch overrides; empty = use Config.REPOSITORIES (see airflow_tasks.REPOSITORY_PARAMS).
    params=REPOSITORY_PARAMS,
)

with orchestrator_test_workflow:
    test_workflow_update_repos = build_update_repos_task(
        dag=orchestrator_test_workflow, env=update_repos_env(), append_env=True,
    )
    test_workflow_install_env = build_install_env_task(dag=orchestrator_test_workflow)
    with TaskGroup('tests') as test_workflow_tests:
        build_tests_task(dag=orchestrator_test_workflow)
    with TaskGroup('to_zarr') as test_workflow_to_zarr:
        build_to_zarr_task(dag=orchestrator_test_workflow)
    with TaskGroup('notebook_docs') as test_workflow_notebook_docs:
        build_notebook_docs_task(dag=orchestrator_test_workflow)

    (
        test_workflow_update_repos
        >> test_workflow_install_env
        >> test_workflow_tests
        >> test_workflow_to_zarr
        >> test_workflow_notebook_docs
    )

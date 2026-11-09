import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, task

from spatialdata_data_converter.airflow_operators import AssetBashOperator, asset_alias
from spatialdata_data_converter.config import Config, branch_envvar, get_cli_command

MAP_INDEX_TEMPLATE = '{{ task.bash_command.split()[-1] }}'

# Per-run branch overrides ("Trigger DAG w/ config"); empty = use Config.REPOSITORIES.
# Same syntax as Config.REPOSITORIES: 'main', 'branch-name', 'pr 329', 'tag v0.4.0', 'None' (latest release).
REPOSITORY_PARAMS = {
    repo: Param(
        None,
        type=["null", "string"],  # nullable, so the trigger form doesn't mark it required
        description=f"Branch / 'pr <n>' / 'tag <t>' / 'None' for {repo}; empty = config.py ({Config.REPOSITORIES[repo]!r})",
    )
    for repo in Config.REPOSITORIES
}


def update_repos_env() -> dict[str, str]:
    """Env for build_update_repos_task in a DAG with REPOSITORY_PARAMS: the per-run overrides.

    Passed via env vars (read by the click options) rather than CLI args, to avoid shell quoting.
    """
    return {branch_envvar(repo): "{{ params['%s'] or '' }}" % repo for repo in REPOSITORY_PARAMS}


TEST_REPOS = ["spatialdata", "spatialdata-plot", "spatialdata-io", "napari-spatialdata"]

DOWNLOADABLE_DATASETS = [d for d in Config.DATASETS if d not in Config.DATASETS_NO_DOWNLOAD]

# 'create_cells_dataset.ipynb' ingests the raw Xenium Prime Cervical Cancer dataset
# (~75 GB on disk), so it is excluded and run manually when needed
EXCLUDED_NOTEBOOKS = ['technology_curio.ipynb', 'technology_stereoseq.ipynb', 'speed_up_illustration.ipynb', 'technology_cosmx.ipynb', '_latest_run_notebook.ipynb', 'create_cells_dataset.ipynb']


def docs_notebooks(missing_ok: bool = True) -> list[str]:
    """The docs notebooks to run (names without .ipynb).

    missing_ok: when a DAG file is parsed (notebooks.py's per-notebook DAGs), a missing notebooks
    checkout -- a fresh instance before update_repos -- gives no notebooks rather than an import
    error. At run time (list_docs_notebooks, after update_repos) a missing checkout is an error.
    """
    if missing_ok and not os.path.isdir(Config.DOCS_NOTEBOOKS_FOLDER):
        return []
    return sorted(
        s[:-len('.ipynb')]
        for s in os.listdir(Config.DOCS_NOTEBOOKS_FOLDER)
        if s.endswith('.ipynb') and s not in EXCLUDED_NOTEBOOKS
    )


def build_update_repos_task(dag, task_group=None, **kwargs):
    # kwargs: e.g. env/append_env for the per-run branch overrides (see install_env.py)
    return BashOperator(
        task_id='update_repos',
        bash_command=get_cli_command('update-repos'),
        dag=dag,
        task_group=task_group,
        retries=0,
        **kwargs,
    )


def build_install_env_task(dag, task_group=None):
    return BashOperator(
        task_id='run_install_env_script',
        bash_command=get_cli_command('install-env'),
        dag=dag,
        task_group=task_group,
        retries=0,
    )


def build_record_env_task(dag, task_group=None):
    # Logs the env's packages and the repo commits a run tests with (#6), as an event on the
    # asset env/ome_sdc (repo commits and package-list hash in its extra).
    return AssetBashOperator(
        task_id='record_env',
        bash_command=get_cli_command('record-env'),
        asset_kind='env',
        asset_item=Config.ENV,
        outlets=[asset_alias('env')],
        dag=dag,
        task_group=task_group,
        retries=0,
    )


def build_create_symlinks_task(dag, task_group=None):
    return BashOperator(
        task_id='create_symlinks',
        bash_command=get_cli_command('create-symlinks'),
        dag=dag,
        task_group=task_group,
        retries=0,
    )


def build_download_spatialdata_io_datasets_task(dag, task_group=None):
    return BashOperator(
        task_id='download_spatialdata_io_datasets',
        bash_command=get_cli_command('download-spatialdata-io-dev-datasets'),
        dag=dag,
        task_group=task_group,
        retries=0,
    )


def build_download_task(dag, task_group=None):
    return AssetBashOperator.partial(
        asset_kind='download',
        outlets=[asset_alias('download')],
        task_id='run_download_script',
        dag=dag,
        task_group=task_group,
        retries=0,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'download --dataset {dataset}') for dataset in DOWNLOADABLE_DATASETS]
    )


def build_tests_task(dag, task_group=None):
    return AssetBashOperator.partial(
        asset_kind='tests',
        outlets=[asset_alias('tests')],
        task_id='run_tests',
        dag=dag,
        task_group=task_group,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'run-tests --repo {repo}') for repo in TEST_REPOS]
    )


def build_to_zarr_task(dag, task_group=None):
    return AssetBashOperator.partial(
        asset_kind='to_zarr',
        outlets=[asset_alias('to_zarr')],
        task_id='run_to_zarr_script',
        dag=dag,
        task_group=task_group,
        retries=0,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'to-zarr --dataset {dataset}') for dataset in Config.DATASETS if dataset not in Config.DATASETS_NO_TO_ZARR]
    )


@task(retries=0)
def list_docs_notebooks() -> list[dict[str, str]]:
    """Run time: the docs notebooks in this run's checkout, as env for run_notebook's mapped instances."""
    return [{"NOTEBOOK": notebook} for notebook in docs_notebooks(missing_ok=False)]


def build_notebook_docs_task(dag, task_group=None):
    # Mapped at run time over what list_docs_notebooks finds after update_repos, not over a list
    # taken when the DAG file was parsed: the checkout that update_repos produces in the same run
    # decides which notebooks run. The command is fixed (only NOTEBOOK varies), so the cluster
    # policy still sees `run-docs-notebook`; it also creates the notebook's symlinks.
    notebooks = list_docs_notebooks.override(dag=dag, task_group=task_group)()
    return AssetBashOperator.partial(
        asset_kind='notebook',
        outlets=[asset_alias('notebook')],
        task_id='run_notebook',
        dag=dag,
        task_group=task_group,
        retries=0,
        bash_command=get_cli_command('run-docs-notebook --notebook "$NOTEBOOK"'),
        append_env=True,
        map_index_template="{{ task.env['NOTEBOOK'] }}",
    ).expand(env=notebooks)

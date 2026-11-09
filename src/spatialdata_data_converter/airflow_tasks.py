import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param

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
def docs_notebooks() -> list[str]:
    # Called when a DAG file is parsed. On a fresh instance the notebooks checkout doesn't exist
    # until update_repos has run (which is a task of the orchestrators that use this), so a
    # missing checkout gives no notebooks rather than an import error; the next parse after
    # update_repos picks them up.
    if not os.path.isdir(Config.DOCS_NOTEBOOKS_FOLDER):
        return []
    return [
        s[:-len('.ipynb')]
        for s in os.listdir(Config.DOCS_NOTEBOOKS_FOLDER)
        if s.endswith('.ipynb') and s not in EXCLUDED_NOTEBOOKS
    ]

# Notebooks that need a symlinker run immediately before them. Keys equal to
# their value are the common case (create-symlinks --dataset X for notebook
# X); the .update() entries are the ones where the notebook name and the
# symlink dataset name differ.
NOTEBOOK_SYMLINK_DATASETS = {
    dataset: dataset
    for dataset in [
        'napari_rois', 'densenet', 'alignment_using_landmarks', 'spatial_query',
        'squidpy_integration', 'technology_merfish', 'technology_mibitof',
        'technology_visium', 'technology_visium_hd', 'technology_visium_hd_mouse_4.0.1',
        'technology_xenium', 'transformations', 'transformations_advanced', 'technology_spacem',
    ]
}
NOTEBOOK_SYMLINK_DATASETS.update({
    'aggregation': 'transformations',
    'intro': 'transformations',
    'table-queries': 'technology_mibitof',
})


def notebook_all_command(notebook: str) -> str:
    notebook_cmd = get_cli_command(f'run-docs-notebook --notebook {notebook}')
    symlink_dataset = NOTEBOOK_SYMLINK_DATASETS.get(notebook)
    if symlink_dataset is None:
        return notebook_cmd
    symlink_cmd = get_cli_command(f'create-symlinks --dataset {symlink_dataset}')
    return f'{symlink_cmd} && {notebook_cmd}'


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
    return BashOperator.partial(
        task_id='run_download_script',
        dag=dag,
        task_group=task_group,
        retries=0,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'download --dataset {dataset}') for dataset in DOWNLOADABLE_DATASETS]
    )


def build_tests_task(dag, task_group=None):
    return BashOperator.partial(
        task_id='run_tests',
        dag=dag,
        task_group=task_group,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'run-tests --repo {repo}') for repo in TEST_REPOS]
    )


def build_to_zarr_task(dag, task_group=None):
    return BashOperator.partial(
        task_id='run_to_zarr_script',
        dag=dag,
        task_group=task_group,
        retries=0,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[get_cli_command(f'to-zarr --dataset {dataset}') for dataset in Config.DATASETS if dataset not in Config.DATASETS_NO_TO_ZARR]
    )


def build_notebook_docs_task(dag, task_group=None):
    return BashOperator.partial(
        task_id='run_notebook',
        dag=dag,
        task_group=task_group,
        retries=0,
        map_index_template=MAP_INDEX_TEMPLATE,
    ).expand(
        bash_command=[notebook_all_command(notebook) for notebook in docs_notebooks()]
    )

from airflow import DAG
from spatialdata_data_converter.config import get_airflow_default_args
from spatialdata_data_converter.airflow_tasks import (
    build_create_symlinks_task,
    build_install_env_task,
    build_update_repos_task,
)

default_args = get_airflow_default_args()

dag = DAG(
    'config_install_env',
    default_args=default_args,
    description='Install the environment for the project',
    schedule=None,
    catchup=False
)

run_shell_script = build_install_env_task(dag=dag)

# --------- update repos DAG ---------
dag_update_repos = DAG(
    'config_update_repos',
    default_args=default_args,
    description='Update all repositories to configured branches',
    schedule=None,
    catchup=False
)

update_repos_task = build_update_repos_task(dag=dag_update_repos)

# --------- create symlinks DAG ---------
dag_create_symlinks = DAG(
    'config_create_symlinks',
    default_args=default_args,
    description='Create symlinks between sandbox datasets and notebooks',
    schedule=None,
    catchup=False
)

create_symlinks_task = build_create_symlinks_task(dag=dag_create_symlinks)
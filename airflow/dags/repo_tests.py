from airflow import DAG
from spatialdata_data_converter.config import get_airflow_default_args, get_cli_command
from spatialdata_data_converter.airflow_operators import AssetBashOperator
from spatialdata_data_converter.airflow_tasks import TEST_REPOS

default_args = get_airflow_default_args()

for repo in TEST_REPOS:
    # --------- run tests for a single repository (all repos: 0_orchestrator_test_workflow) ---------

    dag_id = f'tests_{repo}'

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Run tests {repo}',
        schedule=None,
        catchup=False,
        is_paused_upon_creation=False,
    )

    tests_repo = AssetBashOperator(
        task_id=f'tests_{repo}',
        bash_command=get_cli_command(f'run-tests --repo {repo}'),
        asset_kind='tests',
        dag=dag,
    )

    globals()[f'tests_{repo}'] = tests_repo

    # Assign the dag object to a variable that complies with Airflow's naming conventions
    globals()[dag_id] = dag

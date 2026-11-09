from airflow import DAG
from spatialdata_data_converter.config import get_airflow_default_args, get_cli_command
from spatialdata_data_converter.airflow_operators import AssetBashOperator, asset_alias
from spatialdata_data_converter.airflow_tasks import docs_notebooks

default_args = get_airflow_default_args()

for notebook in docs_notebooks():
    # --------- run a single documentation notebook (all of them: 0_orchestrator_test_workflow) ---------
    dag_id = f'notebook_{notebook}'

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Run notebook {notebook}',
        schedule=None,
        catchup=False,
        is_paused_upon_creation=False,
    )

    # run-docs-notebook creates the symlinks the notebook needs (symlinker.NOTEBOOK_SYMLINK_DATASETS)
    notebook_task = AssetBashOperator(
        task_id=f'notebook_{notebook}',
        bash_command=get_cli_command(f'run-docs-notebook --notebook {notebook}'),
        asset_kind='notebook',
        outlets=[asset_alias('notebook')],
        dag=dag,
        retries=0,
    )

    # Assign the dag object to a variable that complies with Airflow's naming conventions
    globals()[dag_id] = dag

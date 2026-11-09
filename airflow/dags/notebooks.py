from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from spatialdata_data_converter.config import get_airflow_default_args, get_cli_command
from spatialdata_data_converter.airflow_tasks import (
    NOTEBOOK_SYMLINK_DATASETS,
    build_notebook_docs_task,
    docs_notebooks,
)

default_args = get_airflow_default_args()

for notebook in docs_notebooks():
    # --------- run a single documentation notebook ---------
    dag_id = f'notebook_{notebook}'

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Run notebook {notebook}',
        schedule=None,
        catchup=False,
        is_paused_upon_creation=False,
    )

    notebook_task = BashOperator(
        task_id=f'notebook_{notebook}',
        bash_command=get_cli_command(f'run-docs-notebook --notebook {notebook}'),
        dag=dag,
        retries=0,
    )

    # --------- call the symlinker for the notebook's dataset first, if it needs one ---------
    symlink_dataset = NOTEBOOK_SYMLINK_DATASETS.get(notebook)
    if symlink_dataset is not None:
        symlinker = BashOperator(
            task_id=f'symlinker_{notebook}',
            bash_command=get_cli_command(f'create-symlinks --dataset {symlink_dataset}'),
            dag=dag,
        )
        symlinker >> notebook_task

    # Assign the dag object to a variable that complies with Airflow's naming conventions
    globals()[dag_id] = dag

# --------- run all the docs notebooks (lightweight) ---------
notebook_docs_all = DAG(
    'notebook_docs_all',
    default_args=default_args,
    description='Run all the notebook tasks for docs notebooks (lightweight)',
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
)
run_notebook_docs_all = build_notebook_docs_task(dag=notebook_docs_all)

from airflow import DAG
from spatialdata_data_converter.airflow_operators import AssetBashOperator
from spatialdata_data_converter.config import Config, get_airflow_default_args, get_cli_command
from spatialdata_data_converter.airflow_tasks import (
    build_download_spatialdata_io_datasets_task,
    build_download_tasks,
)

default_args = get_airflow_default_args()

# List of datasets
for dataset in Config.DATASETS:
    # --------- download a single dataset ---------
    if dataset not in Config.DATASETS_NO_DOWNLOAD:
        dag_id = f'download_{dataset}'

        dag = DAG(
            dag_id,
            default_args=default_args,
            description=f'Download data for {dataset}',
            schedule=None,
            catchup=False,
            is_paused_upon_creation=False,
        )

        run_download_script = AssetBashOperator(
            task_id=f'run_download_script_{dataset}',
            bash_command=get_cli_command(f'download --dataset {dataset}'),
            asset_kind='download',
            dag=dag,
            retries=0,
        )

        # Assign the dag object to a variable that complies with Airflow's naming conventions
        globals()[dag_id] = dag

    # --------- convert a single dataset to zarr (all of them: 0_orchestrator_test_workflow) ---------
    if dataset not in Config.DATASETS_NO_TO_ZARR:
        dag_id = f'to_zarr_{dataset}'

        dag = DAG(
            dag_id,
            default_args=default_args,
            description=f'Convert data for {dataset} to zarr',
            schedule=None,
            catchup=False,
            is_paused_upon_creation=False,
        )

        run_to_zarr_script = AssetBashOperator(
            task_id=f'run_to_zarr_script_{dataset}',
            bash_command=get_cli_command(f'to-zarr --dataset {dataset}'),
            asset_kind='to_zarr',
            dag=dag,
            retries=0,
        )

        # Assign the dag object to a variable that complies with Airflow's naming conventions
        globals()[dag_id] = dag

    # --------- upload a single dataset ---------
    if dataset not in Config.DATASETS_NO_UPLOAD:
        dag_id = f'upload_{dataset}'

        dag = DAG(
            dag_id,
            default_args=default_args,
            description=f'Upload data for {dataset} to S3',
            schedule=None,
            catchup=False,
            is_paused_upon_creation=True,  # publishes to S3: switched on deliberately per instance
        )

        run_upload_script = AssetBashOperator(
            task_id=f'run_upload_script_{dataset}',
            bash_command=get_cli_command(f'upload --dataset {dataset}'),
            asset_kind='upload',
            dag=dag,
            retries=0,
        )

        # Assign the dag object to a variable that complies with Airflow's naming conventions
        globals()[dag_id] = dag

# --------- download all datasets ---------
# Creation of a 'download_all' DAG that runs a mapped download task per dataset
download_all = DAG(
    'download_all',
    default_args=default_args,
    description='Run all the download tasks',
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
)
run_download_all = build_download_tasks(dag=download_all)

# Also download the spatialdata-io test datasets
run_download_spatialdata_io_datasets = build_download_spatialdata_io_datasets_task(dag=download_all)

# --------- upload all datasets ---------
# Creation of an 'upload_all' DAG that runs a mapped upload task per dataset
upload_all = DAG(
    'upload_all',
    default_args=default_args,
    description='Run all the upload tasks',
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,  # publishes to S3: switched on deliberately per instance
)
# one task per dataset, so each upload shows as its own row (and its own asset event)
run_upload_all = [
    AssetBashOperator(
        task_id=dataset,
        bash_command=get_cli_command(f'upload --dataset {dataset}'),
        asset_kind='upload',
        dag=upload_all,
        retries=0,
    )
    for dataset in Config.DATASETS
    if dataset not in Config.DATASETS_NO_UPLOAD
]

import os
import time
import pendulum
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]


# Just a good practice to follow -> always keep the name of this function same as the file name.
@dag(
        dag_id = "orchestrate",
        schedule = "0 11 * * *",
        catchup = False, # Useful for Backfilling 
        start_date = pendulum.datetime(year=2026, month=6, day=18, tz="UTC") 
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host = DATABRICKS_HOST,
            token = DATABRICKS_TOKEN
        )

        job_trigger = ws.jobs.run_now(job_id=413800041494042)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"

    @task
    def ingest_s3():
        print("Ingestion Logic(for ingesting files via s3 bucket) to be imported via a util")
        print("or you can also mount the s3 bucket directly in databricks as well")
        return "s3 file imported"

    @task.bash 
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"
    
    # We need to run bash commands thats why we are using @task.bash
    @task.bash 
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running
        return "cd /opt/airflow/walmart_project && dbt source freshness"
    
    # It can be done the following way as well using BashOperator :-
    # source_freshness = BashOperator(
    #     task_id = 'source_freshness',
    #     cwd = '/opt/airflow/walmart_project', # Set the working directory for the command 
    #     bash_command = 'dbt source freshness'
    # )

    # In this task we're only running the models under silver_t folder i.e. silver_technical layer 
    silver_technical = BashOperator(
        task_id = 'silver_technical',
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id = 'silver_technical_tests', 
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt test --select silver_t'
    )

    silver_business = BashOperator(
        task_id = 'silver_business', 
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt run --select silver_b'
    )

    silver_business_tests = BashOperator(
        task_id = 'silver_business_tests', 
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt test --select silver_b'
    )

    gold_ephemeral = BashOperator(
        task_id = 'gold_ephemeral',
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt run --select gold/ephemeral'
    )

    gold_dimensions = BashOperator(
        task_id = 'gold_dimensions',
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id = 'gold_facts',
        cwd = '/opt/airflow/walmart_project', 
        bash_command = 'dbt run --select gold/fact'
    )


    # This is how we define the dependency
    [ingest_cdc(), ingest_s3()] >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests  >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

# Now we need to register the dag as well 
orchestrate_dag = orchestrate()
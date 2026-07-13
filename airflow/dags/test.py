import os
import time
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

load_dotenv()  # Loads variables from .env

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

print(repr(DATABRICKS_HOST))
print(repr(DATABRICKS_TOKEN))


ws = WorkspaceClient(
    host = DATABRICKS_HOST,
    token = DATABRICKS_TOKEN
)

job_trigger = ws.jobs.run_now(job_id=413800041494042)

print(job_trigger)

while True:

    job_run = ws.jobs.get_run(job_trigger.run_id)

    if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_run.state.result_state == RunResultState.SUCCESS:
            print("Job completed successfully!")
            break 
        else:
            raise Exception(f"Job failed with state: {job_run.state.result_state}")
            
    time.sleep(5)  # Wait for 5 seconds before checking the job status again
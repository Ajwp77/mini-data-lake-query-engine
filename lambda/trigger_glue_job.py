import boto3
import logging

glue = boto3.client("glue")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = "olist-orders-etl"

ACTIVE_STATES = {
    "STARTING",
    "RUNNING",
    "STOPPING",
    "WAITING"
}


def lambda_handler(event, context):
  
    response = glue.get_job_runs(
        JobName=GLUE_JOB_NAME,
        MaxResults=10
    )

    # Check whether a job is already active
    for job in response.get("JobRuns", []):

        if job["JobRunState"] in ACTIVE_STATES:

            logger.info(
                "Glue job already active: %s",
                job["Id"]
            )

            return {
                "statusCode": 200,
                "message": "Glue job already running",
                "jobRunId": job["Id"]
            }

    # Start a new glue job
    try:

        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME
        )

        logger.info(
            "Started Glue job: %s",
            response["JobRunId"]
        )

        return {
            "statusCode": 200,
            "message": "Glue job started successfully",
            "jobRunId": response["JobRunId"]
        }

    except glue.exceptions.ConcurrentRunsExceededException:

        logger.warning(
            "Glue concurrency limit reached. "
            "No additional job started."
        )

        return {
            "statusCode": 200,
            "message": "Glue job concurrency limit reached"
        }

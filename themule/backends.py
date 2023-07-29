from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING

import boto3

from .conf import settings
from .job import StartedJob

if TYPE_CHECKING:
    from .job import Job
    from .serializers import BaseSerializer


DEFAULT_BACKEND = "themule.backends.AwsBatchBackend"


class BaseBackend:
    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        raise NotImplementedError()

    def get_path(self):
        return f"{self.__module__}.{self.__class__.__name__}"


class AwsBatchBackend(BaseBackend):
    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        serialized_job = serializer.serialize(job)

        queue_name = settings.get_value_for_job(options, "JOB_QUEUE", "queue_name")
        job_definition = settings.get_value_for_job(
            options, "JOB_DEFINITION", "job_definition"
        )

        client = boto3.client("batch")
        response = client.submit_job(
            jobName=str(job.id),
            jobQueue=queue_name,
            jobDefinition=job_definition,
            containerOverrides={
                "command": [
                    "themule",
                    "execute-job",
                    "--serializer",
                    serializer.get_path(),
                    serialized_job,
                ],
            },
        )

        job_id = str(response.get("jobId"))

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )


class LocalThreadBackend(BaseBackend):
    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        thread = Thread(target=job.function, args=job.args, kwargs=job.kwargs)
        thread.start()

        job_id = str(thread.ident) if thread.ident is not None else None

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )

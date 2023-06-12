from __future__ import annotations

from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from .job import Job
    from .serializers import BaseSerializer


class BotoClient:
    def __init__(self) -> None:
        self.client = boto3.client("batch")

    def submit_job(
        self, job: Job, serializer: BaseSerializer, queue_name: str, job_definition: str
    ):
        serialized_job = serializer.serialize(job)

        self.client.submit_job(
            jobName=str(job.id),
            jobQueue=queue_name,
            jobDefinition=job_definition,
            containerOverrides={
                "command": [
                    "themule",
                    "executejob",
                    "--serializer",
                    serializer.get_path(),
                    serialized_job,
                ],
            },
        )

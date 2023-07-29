from __future__ import annotations

import os
from typing import TYPE_CHECKING

from .conf import NOTSET, settings
from .job import StartedJob

if TYPE_CHECKING:
    from .job import Job
    from .serializers import BaseSerializer


DEFAULT_BACKEND = "themule.backends.AwsBatchBackend"


class BaseBackend:
    OPTION_PREFIX = "base"

    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        raise NotImplementedError()

    def get_path(self):
        return f"{self.__module__}.{self.__class__.__name__}"

    def get_option_value(self, options, option, default=NOTSET, cast=None):
        return settings.get_value_for_job(
            options,
            self.OPTION_PREFIX,
            option,
            default=default,
            cast=cast,
        )


class AwsBatchBackend(BaseBackend):
    OPTION_PREFIX = "aws_batch"

    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        try:
            import boto3
        except ImportError:
            raise ValueError("AWS support not installed")

        serialized_job = serializer.serialize(job)

        queue_name = self.get_option_value(options, "queue_name")
        job_definition = self.get_option_value(options, "job_definition")

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


class LocalDockerBackend(BaseBackend):
    OPTION_PREFIX = "docker"

    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        try:
            import docker
        except ImportError:
            raise ValueError("Docker support not installed")

        serialized_job = serializer.serialize(job)

        docker_image = self.get_option_value(options, "image")
        entrypoint = self.get_option_value(
            options, "entrypoint", default=None, cast=list
        )
        environment = self.get_option_value(
            options, "environment", default={}, cast=dict
        )
        pass_environment = self.get_option_value(
            options, "pass_environment", default=True, cast=bool
        )
        auto_remove = self.get_option_value(
            options, "auto_remove", default=True, cast=bool
        )

        if pass_environment:
            environment = {
                **os.environ,
                **environment,
            }

        docker_command = [
            "themule",
            "execute-job",
            "--serializer",
            serializer.get_path(),
            serialized_job,
        ]

        client = docker.from_env()
        container = client.containers.run(
            docker_image,
            docker_command,
            entrypoint=entrypoint,
            environment=environment,
            auto_remove=auto_remove,
            detach=True,
        )

        job_id = container.id

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )


class LocalProcess(BaseBackend):
    def submit_job(self, job: Job, serializer: BaseSerializer, **options) -> StartedJob:
        import subprocess

        serialized_job = serializer.serialize(job)

        command = [
            "themule",
            "execute-job",
            "--serializer",
            serializer.get_path(),
            serialized_job,
        ]

        process = subprocess.Popen(command)

        job_id = process.pid

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )

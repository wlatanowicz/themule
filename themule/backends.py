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

    def __init__(self, **options) -> None:
        pass

    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
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

    def __init__(self, **options) -> None:
        self.queue_name = self.get_option_value(options, "queue_name")
        self.job_definition = self.get_option_value(options, "job_definition")

    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
        try:
            import boto3
        except ImportError:
            raise ValueError("AWS support not installed")

        serialized_job = serializer.serialize(job)

        client = boto3.client("batch")
        response = client.submit_job(
            jobName=str(job.id),
            jobQueue=self.queue_name,
            jobDefinition=self.job_definition,
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

    def __init__(self, **options) -> None:
        self.docker_image = self.get_option_value(options, "image")
        self.entrypoint = self.get_option_value(
            options, "entrypoint", default=None, cast=list
        )
        self.environment = self.get_option_value(
            options, "environment", default={}, cast=dict
        )
        self.pass_environment = self.get_option_value(
            options, "pass_environment", default=True, cast=bool
        )
        self.auto_remove = self.get_option_value(
            options, "auto_remove", default=True, cast=bool
        )
        self.run_options = self.get_option_value(
            options, "run_options", default={}, cast=dict
        )

    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
        try:
            import docker
        except ImportError:
            raise ValueError("Docker support not installed")

        serialized_job = serializer.serialize(job)

        environment = self.environment
        if self.pass_environment:
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

        run_kwargs = {
            "entrypoint": self.entrypoint,
            "environment": environment,
            "auto_remove": self.auto_remove,
            **self.run_options,
        }

        container = client.containers.run(
            self.docker_image,
            docker_command,
            detach=True,
            **run_kwargs,
        )

        job_id = container.id

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )


class LocalProcess(BaseBackend):
    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
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

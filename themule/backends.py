from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator
from uuid import uuid4

from .conf import NOTSET, settings
from .exceptions import ConfigurationError
from .import_helpers import import_by_path
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

    def purge(self):
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

    @dataclass
    class _QueuedJob:
        job_id: str

    def __init__(self, **options) -> None:
        self.queue_name = self.get_option_value(options, "queue_name")
        self.job_definition = self.get_option_value(options, "job_definition")

    def purge(self):
        statuses = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")
        for status in statuses:
            for job in self._list_jobs(status):
                self._terminate_job(job, "Queue purged")

    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
        try:
            import boto3
        except ImportError:
            raise ConfigurationError("AWS support not installed")

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

    def _list_jobs(
        self, status: str | None = None
    ) -> Generator[_QueuedJob, None, None]:
        try:
            import boto3
        except ImportError:
            raise ConfigurationError("AWS support not installed")

        is_first = True
        client = boto3.client("batch")

        while is_first or next_token:
            is_first = False
            kwargs = {}

            if next_token:
                kwargs["nextToken"] = next_token

            if status:
                kwargs["jobStatus"] = status

            result = client.list_jobs(
                jobQueue=self.queue_name,
                **kwargs,
            )
            next_token = result.get("nextToken")
            for job in result.get("jobSummaryList", []):
                yield self._QueuedJob(
                    job_id=job["jobId"],
                )

    def _terminate_job(self, job: _QueuedJob, reason: str):
        try:
            import boto3
        except ImportError:
            raise ConfigurationError("AWS support not installed")

        client = boto3.client("batch")
        client.terminate_job(
            jobId=job.job_id,
            reason=reason,
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
            raise ConfigurationError("Docker support not installed")

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


class Immediate(BaseBackend):
    def submit_job(self, job: Job, serializer: BaseSerializer) -> StartedJob:
        func = import_by_path(job.func)

        func(*job.args, **job.kwargs)
        job_id = str(uuid4())

        return StartedJob(
            self.get_path(),
            job,
            job_id,
        )

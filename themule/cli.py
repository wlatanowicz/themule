import os
from typing import Type

import click

from .executor import execute_job
from .import_helpers import import_by_path
from .serializers import DEFAULT_SERIALIZER, BaseSerializer


@click.group()
def cli():
    pass


@cli.command("execute-job")
@click.option(
    "-s",
    "--serializer",
    "serializer_path",
    type=str,
    help="Path to serializer's class",
)
@click.argument("job-spec", type=str)
def execute_job_cli(serializer_path, job_spec):
    setup = os.environ.get("THEMULE_SETUP_CALLBACK")
    if setup:
        setup_func = import_by_path(setup)
        setup_func()

    if not serializer_path:
        serializer_path = os.environ.get(
            "THEMULE_JOB_SERIALIZER", default=DEFAULT_SERIALIZER
        )
    serializer_class: Type[BaseSerializer] = import_by_path(serializer_path)
    serializer = serializer_class()
    job = serializer.unserialize(job_spec)

    execute_job(job)

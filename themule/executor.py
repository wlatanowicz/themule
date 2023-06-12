from .import_helpers import import_by_path
from .job import Job


def execute_job(job: Job):
    func = import_by_path(job.func)
    func(*job.args, **job.kwargs)

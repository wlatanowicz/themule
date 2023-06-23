from .conf import settings
from .import_helpers import import_by_path
from .job import Job, JobFunction


def execute_job(job: Job):
    func = import_by_path(job.func)

    if settings.STRICT_MODE:
        if not isinstance(func, JobFunction):
            raise ValueError(f"{job.func} is not marked as TheMule job.")

    func(*job.args, **job.kwargs)

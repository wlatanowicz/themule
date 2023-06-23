from .job import JobFunction


def job(**kwargs):
    def inner(fn):
        job_function = JobFunction.from_function(fn, **kwargs)
        return job_function

    return inner

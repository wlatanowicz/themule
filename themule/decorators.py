from .job import JobFunction


def job(**kwargs):
    def inner(fn):
        job_function = JobFunction(fn, **kwargs)
        job_function.augment_function(fn)
        return fn

    return inner

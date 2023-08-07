from time import sleep

from themule import job


@job()
def do_something(job_number):
    print(f"Starting job {job_number}")
    sleep(60)
    print(f"Job finished {job_number}")

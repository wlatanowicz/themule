from time import sleep

from jobs import do_something

if __name__ == "__main__":
    for i in range(5):
        print(f"Submitting job {i}...")
        do_something.submit(job_number=i)
        sleep(30)

The Mule
===

[![tests](https://github.com/wlatanowicz/themule/actions/workflows/tests.yml/badge.svg)](https://github.com/wlatanowicz/themule/actions/workflows/tests.yml)

TheMule is an async task runner for python.

About
---

The purpose of this library is to run long running, heavy tasks in the background using cloud-native solutions. It's modular design allows you to use different backends for individual background tasks.


TheMule vs. Celery
---

TheMule is not intended to replace Celery. They play well together: Celery used to run small and fast tasks, while TheMule is used to run heavy and slow task.


Design principal     | TheMule                         | Celery
---------------------|---------------------------------|--------
Worker lifecycle     | One docker container per task   | Long living worker process
Application codebase | Main application and worker can run from different docker images, but the syntax is simpler when they share the codebase | Main application and worker share codebase
Task assignment      | Tasks can run on different backends; Tasks assignment is done in backend   | Tasks can be assigned to workers with routing
Task queueing        | Backend-specific      | External queueing service (Redis, RabbitMQ)


Available Backends
===

TheMule comes with pre-made backends and you can write your own.


AWS Batch
---

Installation: `pip install themule[aws_batch]`

Class path: `themule.backends.AwsBatchBackend`

Configuration:

Job parameter | Env variable | Required | Default | Description
---|---|---|--|--
aws_batch_queue_name | THEMULE_AWS_BATCH_QUEUE_NAME | Yes | - | The name of AWS Batch queue
aws_batch_job_definition | THEMULE_AWS_BATCH_JOB_DEFINITION | Yes | - | The name of AWS Batch job definition


Local Docker
---

Installation: `pip install themule[docker]`

Class path: `themule.backends.LocalDockerBackend`

Configuration:

Job parameter | Env variable | Required | Default | Description
---|---|---|--|--
docker_image | THEMULE_DOCKER_IMAGE | Yes | - | The name of the Docker image
docker_entrypoint | THEMULE_DOCKER_ENTRYPOINT | No | None | Override of the container's entrypoint
docker_pass_environment | THEMULE_DOCKER_PASS_ENVIRONMENT | No | True | Passes application's env to worker container if true
docker_environment | THEMULE_DOCKER_ENVIRONMENT | No | None | Additional container environment variables
docker_auto_remove | THEMULE_DOCKER_AUTO_REMOVE | No | True | Removes the container after worker exit if true
run_options | THEMULE_DOCKER_RUN_OPTIONS | No | - | Allows to set docker run options

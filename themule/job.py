from dataclasses import dataclass
from threading import Thread
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import UUID, uuid4

from .boto_client import BotoClient
from .conf import settings
from .import_helpers import import_by_path
from .serializers import DEFAULT_SERIALIZER, BaseSerializer


@dataclass
class Job:
    id: UUID
    func: str
    args: List[Any]
    kwargs: Dict[str, Any]
    aws_job_id: Optional[UUID] = None


class JobFunction:
    def __init__(
        self,
        function_path: str,
        *,
        function: Optional[Callable] = None,
        serializer: Optional[Union[BaseSerializer, str]] = None,
        job_definition: Optional[str] = None,
        job_queue: Optional[str] = None,
    ) -> None:
        self.function_path = function_path
        self.function = function
        self.serializer = serializer
        self.job_definition = job_definition
        self.job_queue = job_queue

    @classmethod
    def from_function(
        cls,
        function,
        **kwargs,
    ):
        return cls(
            cls.get_function_path(function),
            function=function,
            **kwargs,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not self.function:
            raise ValueError(
                f"{self.__class__.__name__} object has not function associated with it and cannot be called directly."
            )
        return self.function(*args, **kwargs)

    @staticmethod
    def get_function_path(function):
        if isinstance(function, str):
            return function

        module_path = function.__module__
        function_name = function.__name__
        return f"{module_path}.{function_name}"

    def submit(self, *args, **kwargs):
        job = Job(id=uuid4(), func=self.function_path, args=args, kwargs=kwargs)

        if settings.ALWAYS_EAGER:
            thread = Thread(target=function, args=args, kwargs=kwargs)
            thread.start()
            job.aws_job_id = None
        else:
            boto_client = BotoClient()
            response = boto_client.submit_job(
                job,
                self.get_serializer(),
                self.get_job_queue(),
                self.get_job_definition(),
            )
            aws_job_id = response.get("jobId")
            job.aws_job_id = UUID(aws_job_id)

        return job

    def get_value_from_settings(self, key, default=None, option_name=None):
        assert default is not None or option_name is not None
        value = getattr(settings, key)
        if value is None:
            raise ValueError(
                f"You have to set `{option_name}` in the job decorator or set the system-wide default with `THEMULE_{key}` environmental variable"
            )
        return value

    def get_serializer(self) -> BaseSerializer:
        if isinstance(self.serializer, BaseSerializer):
            return self.serializer

        if isinstance(self.serializer, type) and issubclass(
            self.serializer, BaseSerializer
        ):
            return self.serializer()

        serializer_class_path = self.get_value_from_settings(
            "JOB_SERIALIZER",
            default=DEFAULT_SERIALIZER,
        )
        serializer_class = import_by_path(serializer_class_path)
        assert issubclass(serializer_class, BaseSerializer)
        return serializer_class()

    def get_job_definition(self) -> str:
        if self.job_definition:
            return self.job_definition
        return self.get_value_from_settings(
            "JOB_DEFINITION", option_name="job_definition"
        )

    def get_job_queue(self) -> str:
        if self.job_queue:
            return self.job_queue
        return self.get_value_from_settings("JOB_QUEUE", option_name="job_queue")

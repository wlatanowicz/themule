from dataclasses import dataclass
from os import environ
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from .boto_client import BotoClient
from .import_helpers import import_by_path
from .serializers import DEFAULT_SERIALIZER, BaseSerializer


@dataclass
class Job:
    id: UUID
    func: str
    args: List[Any]
    kwargs: Dict[str, Any]


class JobFunction:
    def __init__(
        self,
        function,
        *,
        serializer: Optional[Union[BaseSerializer, str]] = None,
        job_definition: Optional[str] = None,
        job_queue: Optional[str] = None,
    ) -> None:
        self.function_path = self.get_function_path(function)
        self.serializer = serializer
        self.job_definition = job_definition
        self.job_queue = job_queue

    def augment_function(self, function):
        setattr(function, "_job", self)
        setattr(
            function, "submit", lambda *args, **kwargs: self.submit(*args, **kwargs)
        )

    def get_function_path(self, function):
        if isinstance(function, str):
            return function

        module_path = function.__module__
        function_name = function.__name__
        return f"{module_path}.{function_name}"

    def submit(self, *args, **kwargs):
        job = Job(id=uuid4(), func=self.function_path, args=args, kwargs=kwargs)

        boto_client = BotoClient()
        boto_client.submit_job(
            job,
            self.get_serializer(),
            self.get_job_queue(),
            self.get_job_definition(),
        )

    def get_value_from_environ(self, key, default=None, option_name=None):
        assert default is not None or option_name is not None
        value = environ.get(key, default=default)
        if value is None:
            raise ValueError(
                f"You have to set `{option_name}` in the job decorator or set the system-wide default with `{key}` environmental variable"
            )
        return value

    def get_serializer(self) -> BaseSerializer:
        if isinstance(self.serializer, BaseSerializer):
            return self.serializer

        if isinstance(self.serializer, type) and issubclass(
            self.serializer, BaseSerializer
        ):
            return self.serializer()

        serializer_class_path = self.get_value_from_environ(
            "THEMULE_JOB_SERIALIZER",
            default=DEFAULT_SERIALIZER,
        )
        serializer_class = import_by_path(serializer_class_path)
        assert issubclass(serializer_class, BaseSerializer)
        return serializer_class()

    def get_job_definition(self) -> str:
        if self.job_definition:
            return self.job_definition
        return self.get_value_from_environ(
            "THEMULE_JOB_DEFINITION", option_name="job_definition"
        )

    def get_job_queue(self) -> str:
        if self.job_queue:
            return self.job_queue
        return self.get_value_from_environ("THEMULE_JOB_QUEUE", option_name="job_queue")

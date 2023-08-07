from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union
from uuid import UUID, uuid4

from .conf import settings
from .import_helpers import import_by_path

if TYPE_CHECKING:
    from .backends import BaseBackend
    from .serializers import BaseSerializer


@dataclass
class Job:
    id: UUID
    func: str
    args: List[Any]
    kwargs: Dict[str, Any]


@dataclass
class StartedJob:
    backend_class: str
    job: Job
    job_id: Optional[str] = None


class JobFunction:
    def __init__(
        self,
        function_path: str,
        *,
        function: Optional[Callable] = None,
        serializer: Optional[Union[BaseSerializer, str]] = None,
        backend: Optional[Union[BaseBackend, str]] = None,
        **kwargs,
    ) -> None:
        self.function_path = function_path
        self.function = function
        self.serializer = serializer
        self.backend = backend
        self.additional_kwargs = kwargs

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

    def submit(self, *args, **kwargs) -> StartedJob:
        job = Job(id=uuid4(), func=self.function_path, args=args, kwargs=kwargs)

        serializer = self.get_serializer(self.additional_kwargs)
        backend = self.get_backend(self.additional_kwargs)

        started_job = backend.submit_job(
            job,
            serializer,
        )
        return started_job

    def get_serializer(self, options) -> BaseSerializer:
        from .serializers import BaseSerializer

        if isinstance(self.serializer, BaseSerializer):
            return self.serializer

        if isinstance(self.serializer, type) and issubclass(
            self.serializer, BaseSerializer
        ):
            return self.serializer(**options)

        serializer_class_path = settings.JOB_SERIALIZER
        serializer_class = import_by_path(serializer_class_path)
        assert issubclass(serializer_class, BaseSerializer)
        return serializer_class(**options)

    def get_backend(self, options) -> BaseBackend:
        from .backends import BaseBackend

        if isinstance(self.backend, BaseBackend):
            return self.backend

        if isinstance(self.backend, type) and issubclass(self.backend, BaseBackend):
            return self.backend(**options)

        backend_class_path = settings.BACKEND
        backend_class = import_by_path(backend_class_path)
        assert issubclass(backend_class, BaseBackend)
        return backend_class(**options)

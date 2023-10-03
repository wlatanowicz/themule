from __future__ import annotations

import json
from datetime import date, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from .conf import NOTSET, settings

if TYPE_CHECKING:
    from .job import Job


DEFAULT_SERIALIZER = "themule.serializers.JsonSerializer"


class BaseSerializer:
    def __init__(self, **options) -> None:
        pass

    def serialize(self, job: Job) -> str:
        raise NotImplementedError()

    def unserialize(self, data: str) -> Job:
        raise NotImplementedError()

    def get_path(self):
        return f"{self.__module__}.{self.__class__.__name__}"

    def get_option_value(self, options, option, default=NOTSET, cast=None):
        return settings.get_value_for_job(
            options,
            self.OPTION_PREFIX,
            option,
            default=default,
            cast=cast,
        )


class JsonSerializer(BaseSerializer):
    def serialize(self, job: Job) -> str:
        payload = {
            "id": str(job.id),
            "func": job.func,
            "args": job.args,
            "kwargs": job.kwargs,
        }
        return json.dumps(
            payload,
            default=self._json_serializer,
        )

    def unserialize(self, data: str) -> Job:
        from .job import Job

        json_payload = json.loads(data)
        return Job(
            id=UUID(json_payload["id"]),
            func=json_payload["func"],
            args=json_payload["args"],
            kwargs=json_payload["kwargs"],
        )

    def _json_serializer(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()

        raise TypeError(f"Type {type(obj)} is not JSON serializable")

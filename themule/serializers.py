from __future__ import annotations

import json
from datetime import date, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from .conf import NOTSET, settings
from .exceptions import ConfigurationError

if TYPE_CHECKING:
    from .job import Job


DEFAULT_SERIALIZER = "themule.serializers.JsonSerializer"


class BaseSerializer:
    OPTION_PREFIX = None

    def __init__(self, **options) -> None:
        pass

    def serialize(self, job: Job) -> str:
        raise NotImplementedError()

    def unserialize(self, data: str) -> Job:
        raise NotImplementedError()

    def cleanup(self, job: Job):
        pass

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


class RedisStoreSerializer(BaseSerializer):
    OPTION_PREFIX = "redis_store"

    DEFAULT_PREFIX = "themule_job/"
    DEFAULT_TTL = 60 * 60 * 24 * 30  # 30 days
    DEFAULT_CLEANUP_TTL = 600  # 10 minutes

    def __init__(self, **options) -> None:
        self.redis_url = self.get_option_value(options, "url")
        self.ttl = self.get_option_value(
            options, "ttl", default=self.DEFAULT_TTL, cast=int
        )
        self.cleanup_ttl = self.get_option_value(
            options, "cleanup_ttl", default=self.DEFAULT_CLEANUP_TTL, cast=int
        )
        self.prefix = self.get_option_value(
            options, "prefix", default=self.DEFAULT_PREFIX, cast=str
        )

    def _make_key(self, job: Job) -> str:
        return f"{self.prefix}{job.id}"

    def serialize(self, job: Job) -> str:
        try:
            import redis
        except ImportError:
            raise ConfigurationError("Redis support not installed")

        conn = redis.from_url(self.redis_url)

        payload = {
            "id": str(job.id),
            "func": job.func,
            "args": job.args,
            "kwargs": job.kwargs,
        }
        json_payload = json.dumps(
            payload,
            default=self._json_serializer,
        )

        key = self._make_key(job)
        conn.setex(key, json_payload, self.ttl)

    def unserialize(self, data: str) -> Job:
        try:
            import redis
        except ImportError:
            raise ConfigurationError("Redis support not installed")
        from .job import Job

        conn = redis.from_url(self.redis_url)

        key = data
        payload = conn.get(key)

        json_payload = json.loads(payload)
        job = Job(
            id=UUID(json_payload["id"]),
            func=json_payload["func"],
            args=json_payload["args"],
            kwargs=json_payload["kwargs"],
        )

        key_check = self._make_key(job)
        assert key == key_check
        return job

    def cleanup(self, job: Job):
        try:
            import redis
        except ImportError:
            raise ConfigurationError("Redis support not installed")

        conn = redis.from_url(self.redis_url)
        key = self._make_key(job)
        conn.expire(key, self.cleanup_ttl)

    def _json_serializer(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()

        raise TypeError(f"Type {type(obj)} is not JSON serializable")

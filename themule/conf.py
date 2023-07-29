from os import environ

from .backends import DEFAULT_BACKEND
from .serializers import DEFAULT_SERIALIZER


class Settings:

    _ENV_PREFIX = "THEMULE_"

    def _get_from_env(self, name, default=None):
        return environ.get(f"{self._ENV_PREFIX}{name}", default=default)

    def get_value_for_job(self, options, key, option_name, default=None):
        value = options.get(option_name)
        if value is not None:
            return value

        value = self._get_from_env(key, default=default)
        if value is None:
            raise ValueError(
                f"You have to set `{option_name}` in the job decorator or set the system-wide default with `{self._ENV_PREFIX}{key}` environmental variable"
            )
        return value

    @property
    def BOOTSTRAP_CALLBACK(self):
        return self._get_from_env("BOOTSTRAP_CALLBACK")

    @property
    def BACKEND(self):
        return self._get_from_env("BACKEND", default=DEFAULT_BACKEND)

    @property
    def JOB_SERIALIZER(self):
        return self._get_from_env("JOB_SERIALIZER", default=DEFAULT_SERIALIZER)

    @property
    def STRICT_MODE(self):
        return self._get_from_env("STRICT_MODE", default=True)


settings = Settings()

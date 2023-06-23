from os import environ

from .serializers import DEFAULT_SERIALIZER


class Settings:

    _ENV_PREFIX = "THEMULE_"

    def _get_from_env(self, name, default=None):
        return environ.get(f"{self._ENV_PREFIX}{name}", default=default)

    @property
    def BOOTSTRAP_CALLBACK(self):
        return self._get_from_env("BOOTSTRAP_CALLBACK")

    @property
    def JOB_SERIALIZER(self):
        return self._get_from_env("JOB_SERIALIZER", default=DEFAULT_SERIALIZER)

    @property
    def JOB_DEFINITION(self):
        return self._get_from_env("JOB_DEFINITION")

    @property
    def JOB_QUEUE(self):
        return self._get_from_env("JOB_QUEUE")

    @property
    def ALWAYS_EAGER(self):
        return self._get_from_env("ALWAYS_EAGER", default=False)

    @property
    def STRICT_MODE(self):
        return self._get_from_env("STRICT_MODE", default=True)


settings = Settings()

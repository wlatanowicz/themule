import environ
from environ.compat import ImproperlyConfigured

NOTSET = environ.Env.NOTSET


class Settings:

    _ENV_PREFIX = "THEMULE_"

    def __init__(self) -> None:
        self.env = environ.Env()

    def _get_from_env(self, name, default=NOTSET, cast=None):
        return self.env(f"{self._ENV_PREFIX}{name}", default=default, cast=cast)

    def get_value_for_job(
        self, options, prefix, option_name: str, default=NOTSET, cast=None
    ):
        prefixed_option_name = f"{prefix}_{option_name}"
        value = options.get(prefixed_option_name)
        if value is not None:
            return value

        key = prefixed_option_name.upper()
        try:
            value = self._get_from_env(key, default=default, cast=cast)
        except ImproperlyConfigured:
            raise ImproperlyConfigured(
                f"You have to set `{option_name}` in the job decorator or set the system-wide default with `{self._ENV_PREFIX}{key}` environmental variable"
            )
        return value

    @property
    def BOOTSTRAP_CALLBACK(self):
        return self._get_from_env("BOOTSTRAP_CALLBACK", default=None)

    @property
    def BACKEND(self):
        from .backends import DEFAULT_BACKEND

        return self._get_from_env("BACKEND", default=DEFAULT_BACKEND)

    @property
    def JOB_SERIALIZER(self):
        from .serializers import DEFAULT_SERIALIZER

        return self._get_from_env("JOB_SERIALIZER", default=DEFAULT_SERIALIZER)

    @property
    def STRICT_MODE(self):
        return self._get_from_env("STRICT_MODE", default=True)


settings = Settings()

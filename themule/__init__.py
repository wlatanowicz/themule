from os import path

from .decorators import job  # pylint: disable=unused-import
from .job import Job  # pylint: disable=unused-import

with open(path.join(path.dirname(__file__), "VERSION")) as f:
    __version__ = f.read().strip()

__author__ = "Wiktor Latanowicz"

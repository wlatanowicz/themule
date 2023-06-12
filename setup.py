import re
from pathlib import Path
from os import path

from setuptools import find_packages, setup


def strip_comments(l):
    return l.split("#", 1)[0].strip()


def _pip_requirement(req, *root):
    if req.startswith("-r "):
        _, path = req.split()
        return reqs(*root, *path.split("/"))
    return [req]


def _reqs(*f):
    path = (Path.cwd() / "requirements").joinpath(*f)
    with path.open() as fh:
        reqs = [strip_comments(l) for l in fh.readlines()]
        return [_pip_requirement(r, *f[:-1]) for r in reqs if r]


def reqs(*f):
    return [req for subreq in _reqs(*f) for req in subreq]


def long_description():
    with open("README.md", "r") as fh:
        return fh.read()


def get_about():
    """Parses __init__ on main module in search of all dunder names"""
    regex = re.compile(r"^__\w+__\s*=.*$")
    about = dict()
    with open("themule/__init__.py", "r") as f:
        dunders = list()
        for l in f.readlines():
            if regex.match(l):
                dunders.append(l)
        exec("\n".join(dunders), about)

    with open(path.join(path.dirname(__file__), "themule", "VERSION")) as f:
        about["__version__"] = f.read().strip()

    return about

about = get_about()


setup(
    name="themule",
    version=about["__version__"],
    description="The Mule - run arbitrary python function on AWS Batch",
    url="http://github.com/wlatanowicz/themule",
    author=about["__author__"],
    author_email="wiktor@latanowicz.com",
    license="MIT",
    long_description=long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests*",]),
    zip_safe=False,
    install_requires=reqs("base.txt"),
    tests_require=reqs("tests.txt"),
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    test_suite="tests",
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'themule = themule.__main__:main'
        ]
    },
)

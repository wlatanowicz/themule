import importlib


def from_module_import_object(module_name: str, object_name: str):
    return getattr(importlib.import_module(module_name), object_name)


def import_by_path(path: str):
    if "." not in path:
        return importlib.import_module(path)

    module_name, object_name = path.rsplit(".", maxsplit=1)
    return from_module_import_object(module_name, object_name)

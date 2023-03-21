"""pyyaml loader."""
from typing import Any

import yaml

from .yaml_constructors import constructors_dict
from .yaml_executers import returners_dict


def get_loader():
    """Add constructors and returners to PyYAML loader."""
    loader = yaml.UnsafeLoader

    for tag, constructor in constructors_dict.items():
        loader.add_constructor(tag, constructor)

    for tag, returner in returners_dict.items():
        loader.add_constructor(tag, returner)

    return loader


def load_yaml(yaml_file: str, loader=yaml.SafeLoader) -> Any:
    """
    Load an yaml yaml file.

    Parameters
    ----------
    yaml_file
        Path to the yaml file.
    loader
        PyYAML loader.

    Returns
    -------
        yaml file content.
    """
    with open(yaml_file, "r") as stream:
        try:
            yaml_contents = yaml.load(stream, Loader=loader)
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(exc)

    return yaml_contents

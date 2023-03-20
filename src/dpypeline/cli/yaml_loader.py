"""pyyaml loader."""
import yaml

from .yaml_constructors import constructors_dict
from .yaml_executers import returners_dict


def get_loader():
    """Add constructors and returners to PyYAML loader."""
    loader = yaml.SafeLoader

    for tag, constructor in constructors_dict.items():
        loader.add_constructor(tag, constructor)

    for tag, returner in returners_dict.items():
        loader.add_constructor(tag, returner)

    return loader

from typing import Callable
from .actions import subscribe_action, Action


def extract_from_server(event, data, *args, **kwargs):
    print(event, data, args, kwargs)
    pass


def setup_extract_from_server(*args, **kwargs) -> None:
    setup_extract_action(extract_from_server, *args, **kwargs)


def setup_extract_action(extract_func: Callable, *args, **kwargs) -> None:
    """
    Sets up an extract action.

    Parameters
    ----------
    extract_func
        Function that executes the extract action.
    args
        Arguments to be passed to the function that executes the extract action.
    kwargs
        Keyword arguments to be passed to the function that executes the extract action.
    """
    subscribe_action(Action.EXTRACT, extract_func, args, kwargs)


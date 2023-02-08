from typing import Callable

from .actions_base import Action, subscribe_action


def setup_extract_action(extract_obj: Callable, *args, **kwargs) -> None:
    """
    Sets up an extract action.

    Parameters
    ----------
    extract_obj
        Callable object that executes the extract action.
    args
        Arguments to be passed to the function that executes the extract action.
    kwargs
        Keyword arguments to be passed to the function that executes the extract action.
    """
    subscribe_action(Action.EXTRACT, extract_obj, args, kwargs)

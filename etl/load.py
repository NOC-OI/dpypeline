from typing import Callable

from .actions_base import Action, subscribe_action


def setup_load_action(load_obj: Callable, *args, **kwargs) -> None:
    """
    Sets up a load action.

    Parameters
    ----------
    load_obj
        Callable object that executes the load action.
    args
        Arguments to be passed to the function that executes the load action.
    kwargs
        Keyword arguments to be passed to the function that executes the load action.
    """
    subscribe_action(Action.LOAD, load_obj, args, kwargs)

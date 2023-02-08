from typing import Callable

from .actions_base import Action, subscribe_action


def setup_transform_action(transform_obj: Callable, *args, **kwargs) -> None:
    """
    Sets up a transform action.

    Parameters
    ----------
    transform_obj
        Callable object that executes the transform action.
    args
        Arguments to be passed to the function that executes the transform action.
    kwargs
        Keyword arguments to be passed to the function that executes the transform action.
    """
    subscribe_action(Action.TRANSFORM, transform_obj, args, kwargs)

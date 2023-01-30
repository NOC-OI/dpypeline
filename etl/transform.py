from typing import Callable
from .actions import subscribe_action, Action


def setup_transform_action(transform_func: Callable, *args, **kwargs) -> None:
    """
    Sets up a transform action.

    Parameters
    ----------
    transform_func
        Function that executes the transform action.
    args
        Arguments to be passed to the function that executes the transform action.
    kwargs
        Keyword arguments to be passed to the function that executes the transform action.
    """
    subscribe_action(Action.TRANSFORM, transform_func, args, kwargs)

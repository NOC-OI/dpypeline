import logging
from typing import Callable
from typing import Any
from enum import Enum, auto

actions = dict()


class Action(Enum):
    """
    Action types.
    """
    EXTRACT = auto()
    TRANSFORM = auto()
    LOAD = auto()


def subscribe_action(act: Action, action_func: Callable, actions_args: tuple = (), actions_kwargs: dict = {}) -> dict:
    """
    Subscribe an action to the actions' dictionary.

    Parameters
    ----------
    act
        Action to subscribe to (Action.EXTRACT, Action.TRANSFORM, Action.LOAD).
    action_func
        Function associated with the action.
    actions_args
        Arguments to be passed to the function.
    actions_kwargs
        Keyword arguments to be passed to the function.

    Returns
    -------
    actions
        Dictionary containing mapping actions to functions.
    """
    if act not in actions:
        actions[act] = []

    act_dict = {"function": action_func, "args": actions_args, "kwargs": actions_kwargs}
    actions[act].append(act_dict)

    return actions


def post_action(act: Action, data: Any) -> None:
    """
    Execute the functions associated with a given action.

    Parameters
    ----------
    act
        Action type to perform (e.g., Action.EXTRACT, Action.TRANSFORM, Action.LOAD)
    data
        Data to pass to the functions associated with action_type.
    """
    if act not in actions:
        logging.info(f"Action {act} is not in the actions dictionary.")
        return

    for proc in actions[act]:
        proc["function"](data, *proc["args"], **proc["kwargs"])

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


def subscribe_action(act: Action, action_func: Callable) -> dict:
    """
    Subscribe an action to the actions' dictionary.

    Parameters
    ----------
    act
        Action to subscribe to (Action.EXTRACT, Action.TRANSFORM, Action.LOAD).
    action_func
        Function associated with the action.

    Returns
    -------
    actions
        Dictionary containing mapping actions to functions.
    """
    if act not in actions:
        actions[act] = []

    actions[act].append(action_func)

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

    for func in actions[act]:
        func(data)

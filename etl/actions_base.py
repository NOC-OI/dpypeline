import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, List


class Action(Enum):
    """
    Action types.
    """

    EXTRACT = auto()
    TRANSFORM = auto()
    LOAD = auto()


actions: Dict[Action, List[Dict[str, Any]]] = dict()


class ActionExecutor(ABC):
    """
    Abstract class of ActionExecutor.
    All actions must inherit from this class.

    Parameters
    ----------
    _dependencies
        Actions on which this action depends
    _output
        Output of this action. `None` if action has not been run yet.
    """

    def __init__(self, dependencies: list[ActionExecutor] | None = None):
        """
        Parameters
        ----------
        dependencies
            Actions on which this action depends
        """
        self._dependencies = dependencies or []
        self._output = None

    def add_dependency(self, dependency: ActionExecutor) -> list[ActionExecutor]:
        """
        Adds a dependency to this action.

        Parameters
        ----------
        dependency
            Dependency to add to the action.
        Returns
        -------
        _dependencies
            List of dependencies.
        """
        self._dependencies.append(dependency)
        return self._dependencies

    def retrieve_dependencies_output(self) -> list[Any]:
        """
        Retrieves all the outputs from the dependencies.

        Returns
        -------
        List of outputs in the same order as the dependencies.
        """
        return [dependency._output for dependency in self._dependencies]

    def __call__(self, event: Any, *args, **kwargs):
        """
        Actions should be called as Action(event, arg1, arg2, ..., kwarg1=..., kwarg2=, ...)

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.

        Returns
        -------
        _output
            Action output.
        """
        self._output = self._run_action(event, *args, **kwargs)

        return self._output

    @abstractmethod
    def _run_action(self, *args, **kwargs) -> Any:
        """
        Abstract method.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.

        Returns
        -------
        Action output.
        """
        pass


def subscribe_action(
    act: Action,
    action_func: Callable,
    actions_args: tuple = (),
    actions_kwargs: dict = {},
) -> dict:
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


def post_action(act: Action, event) -> None:
    """
    Execute the functions associated with a given action.

    Parameters
    ----------
    act
        Action type to perform (e.g., Action.EXTRACT, Action.TRANSFORM, Action.LOAD)
    event
        Event representing file or directory creation.
    """
    if act not in actions:
        logging.info(f"{act} is not in the actions dictionary.")
        return

    for proc in actions[act]:
        logging.info(
            f"{act}: calling instance of {proc['function'].__class__.__name__} class | Event: {event}"
        )
        proc["function"](event, *proc["args"], **proc["kwargs"])
        logging.info(
            f"{act}: finished executing instance of {proc['function'].__class__.__name__} class | Event: {event}"
        )

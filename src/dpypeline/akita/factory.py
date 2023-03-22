"""Akita dependency factory."""

from watchdog.observers.polling import PollingObserver

from .directory_state import DirectoryState
from .event_handler import EventHandler
from .queue_events import EventsQueue


class AkitaFactory:
    """Factory that creates instances required for Akita."""

    def __init__(
        self,
        path: str = "./",
        patterns: str | list[str] = ["*.nc"],
        ignore_patterns: str | list[str] = None,
        ignore_directories: bool = True,
        case_sensitive: bool = True,
        glob_kwargs: dict = None,
    ) -> None:
        """
        Initialize the factory.

        Parameters
        ----------
        path, optional
            _description_, by default "./"
        patterns, optional
            Patterns to allow matching events, by default ["*.nc"]
        ignore_patterns, optional
            Patterns to ignore matching event paths, by default True`
        ignore_directories, optional
            If True directories are ignored, by default True
        case_sensitive, optional
            If True path names are matched sensitive to case, by default True
        glob_kwargs, optional
            Kwargs to pass to glob, by default None
        """
        self._path = path
        self._patterns = list(patterns) if patterns is str else patterns
        self._ignore_patterns = (
            list(ignore_patterns) if ignore_patterns is str else ignore_patterns
        )
        self._ignore_directories = ignore_directories
        self._case_sensitive = case_sensitive
        self._glob_kwargs = glob_kwargs if glob_kwargs is not None else {}

    def get_queue(self, maxsize=0) -> EventsQueue:
        """
        Create the queue.

        Parameters
        ----------
        maxsize
            Maximum size of the queue.

        Returns
        -------
            EventsQueue instance.
        """
        return EventsQueue(maxsize)

    def get_event_handler(
        self,
        queue: EventsQueue,
        patterns: str | list[str] = None,
        ignore_patterns: str | list[str] = None,
        ignore_directories: bool = None,
        case_sensitive: bool = None,
    ) -> EventHandler:
        """
        Create the event handler.

        Notes
        -----
        Create the handler responsible for matching given patterns with file paths
        associated with occurring events.

        Parameters
        ----------
        patterns
            Patterns to allow matching events.
        ignore_patterns
            Patterns to ignore matching event paths.
        ignore_directories
            If `True` directories are ignored; `False` otherwise.
        case_sensitive
            If `True` path names are matched sensitive to case; `False` otherwise.

        Returns
        -------
            EvetnHandler instance.
        """
        patterns = patterns if patterns is not None else self._patterns
        ignore_patterns = (
            ignore_patterns if ignore_patterns is not None else self._ignore_patterns
        )
        ignore_directories = (
            ignore_directories
            if ignore_directories is not None
            else self._ignore_directories
        )
        case_sensitive = (
            case_sensitive if case_sensitive is not None else self._case_sensitive
        )

        return EventHandler(
            queue, patterns, ignore_patterns, ignore_directories, case_sensitive
        )

    def get_polling_observer(self) -> PollingObserver:
        """
        Create the polling observer.

        Returns
        -------
            PollingObserver instance.
        """
        return PollingObserver()

    def get_directory_state(
        self,
        path: str = None,
        patterns: str | list[str] = None,
        glob_kwargs: dict = None,
    ) -> DirectoryState:
        """
        Create the directory state.

        Parameters
        ----------
        path
            Path to watch.
        patterns, optional
            Patterns to allow matching events.
        glob_kwargs, optional
            Kwargs to pass to glob.

        Returns
        -------
            DirectoryState instance.
        """
        path = path if path is not None else self._path
        patterns = patterns if patterns is not None else self._patterns
        glob_kwargs = glob_kwargs if glob_kwargs is not None else self._glob_kwargs

        return DirectoryState(path, patterns, glob_kwargs)


def get_akita_dependencies(
    path: str = "./",
    patterns: str | list[str] = ["*.nc"],
    ignore_patterns: str | list[str] = None,
    ignore_directories: bool = True,
    case_sensitive: bool = True,
    glob_kwargs: dict = None,
) -> tuple[str, EventsQueue, EventHandler, DirectoryState, PollingObserver]:
    """
    Get the dependencies of Akita.

    Returns
    -------
        List of dependencies of Akita.
    """
    akita_factory = AkitaFactory(
        path, patterns, ignore_patterns, ignore_directories, case_sensitive, glob_kwargs
    )
    queue = akita_factory.get_queue()
    event_handler = akita_factory.get_event_handler(queue)
    directory_state = akita_factory.get_directory_state()
    polling_observer = akita_factory.get_polling_observer()

    return path, queue, event_handler, directory_state, polling_observer

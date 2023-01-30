import time
from queue import Queue

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver


class FileHandler(PatternMatchingEventHandler):
    """
    Watchdog that sniffs and guards a filesystem.
    """

    def __init__(self, patterns: str | list[str] = None, ignore_patterns: str | list[str] | None = None,
                 ignore_directories: bool = False, case_sensitive: bool = True, queue: Queue = Queue()):
        super().__init__(patterns=patterns,
                         ignore_patterns=ignore_patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive)

        self._queue = queue

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, queue):
        self._queue = queue

    @queue.deleter
    def queue(self):
        del self._queue

    def on_created(self, event):
        self._queue.put(event)

        return event

    def on_deleted(self, event):
        pass

    def on_modified(self, event):
        print(event)

    def on_moved(self, event):
        print(event)


class Akita:
    def __init__(self,
                 path: str,
                 observer: Observer | PollingObserver = PollingObserver(),
                 queue: Queue = Queue()):

        self._path = path
        self.observer = observer
        self.queue = queue
        self.event_handler = None

    def _create_event_handler(self, patterns: str | list[str] = ["*.nc"], ignore_patterns: str | list[str] | None = None,
                              ignore_directories: bool = True, case_sensitive: bool = True) -> FileHandler:

        self.event_handler = FileHandler(patterns=patterns,
                                         ignore_patterns=ignore_patterns,
                                         ignore_directories=ignore_directories,
                                         case_sensitive=case_sensitive)

        return self.event_handler

    def run(self, patterns: str | list[str] = ["*.nc"], ignore_patterns: str | list[str] | None = None,
            ignore_directories: bool = True, case_sensitive: bool = True):

        if self.event_handler is None:
            self._create_event_handler(patterns=patterns,
                                       ignore_patterns=ignore_patterns,
                                       ignore_directories=ignore_directories,
                                       case_sensitive=case_sensitive)

        self.observer.schedule(self.event_handler, self.path, recursive=True)
        self.observer.start()

        try:
            while True:
                time.sleep(1)
        finally:
            self.observer.stop()

        self.observer.join()


if __name__ == "__main__":
    import fsspec
    import fsspec.fuse

    jasmin_login = fsspec.filesystem("sftp", host="login3.jasmin.ac.uk", username="jmorado")
    mount_point = "/home/joaomorado/fsspec_mnt/"
    fsspec.fuse.run(fs=jasmin_login, path="test", mount_point=mount_point)
    akita_watchdog = Akita(watch_dir=mount_point)
    akita_watchdog.run()

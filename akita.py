import time
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


class Akita(PatternMatchingEventHandler):
    """
    Watchdog that sniffs and guards a filesystem.
    """

    def __init__(self, patterns: str | list[str] = ["*.nc"], ignore_patterns: str | list[str] | None = None,
                 ignore_directories: bool = True, case_sensitive: bool = True):
        super().__init__(patterns=patterns,
                         ignore_patterns=ignore_patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive)

    def on_created(self, event):
        print(event)

    def on_deleted(self, event):
        print(event)

    def on_modified(self, event):
        print(event)

    def on_moved(self, event):
        print(event)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    event_handler = Akita()
    observer = Observer()
    observer.schedule(event_handler, "./", recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()

import time
from threading import Thread

from .actions_base import Action, post_action
from .etl_base import ETLBase


class ETLPipeline(ETLBase):
    """
    Extract-load-transform (ETL) pipeline.

    This pipeline relies on a watchdog that continuously monitors a folder to check for any events.
    The watchdog signals these events to a worker thread that is created in this pipeline.
    When triggered, this worker thread executes the ETL pipeline step by step.

    Attributes
    ----------
    _watchdog
        Instance of the watchdog object used to monitor the system.
    _worker: Thread
        Instance of the worker thread that performs the ETL pipeline.
    """

    def __init__(self, watchdog, worker: Thread | None = None) -> None:
        """
        Init method for ETLPipeline.

        Parameters
        ----------
        watchdog
            Instance of a watchdog object used to monitor the system.
        worker : :obj:`Thread`, optional
            Instance of the worker thread that performs the ETL pipeline.
        """
        self._watchdog = watchdog
        self._worker = worker

    def _extract(self, event) -> None:
        """
        Extracts the data from target sources.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.
        """
        post_action(Action.EXTRACT, event)

    def _transform(self, event) -> None:
        """
        Transforms the raw data extracted from the sources into a final desired format.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.
        """
        post_action(Action.TRANSFORM, event)

    def _load(self, event) -> None:
        """
        Writes the transformed data from a staging area to a target destination.

        Parameters
        ----------
        event
            Event representing file or directory creation, deletion, modification or moving.
        """
        post_action(Action.LOAD, event)

    def _run_worker(self, sleep_time: int = 1) -> None:
        """
        Callback function to be used as a target by Thread.

        Parameters
        ----------
        sleep_time
            Sleep time in seconds for which the thread is idle.
        """
        # Run the watchdog
        while True:
            if self._watchdog.get_number_events():
                # Trigger the ETL pipeline
                event = self._watchdog.get_event()
                self._extract(event)
                self._transform(event)
                self._load(event)
            else:
                time.sleep(sleep_time)

    def _create_worker(self, daemon: bool = True) -> Thread:
        """
        Creates the thread worker that executes the ETL pipeline.

        Parameters
        ----------
        daemon
            If `True`, runs the thread as a daemon; otherwise thread is not created as a daemon.

        Returns
        -------
        worker
            Worker thread that executes the ETL pipeline.
        """
        # Set up a worker thread to process database load
        self._worker = Thread(target=self._run_worker, daemon=daemon)

        return self._worker

    def run(self) -> None:
        """
        Runs the ETL pipeline.

        Returns
        -------
        None
        """
        try:
            if self._worker is None:
                self._create_worker()

            self._worker.start()
            self._watchdog.run()
        except Exception as error:
            self._worker.join()

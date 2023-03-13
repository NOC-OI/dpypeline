"""Decorators for thread-based tasks."""
import functools
import logging
import time
from typing import Any, Callable


def retry(
    _func: Callable | None = None, *, max_retries: int = 3, sleep_time: int = 5
) -> Callable:
    """
    Retry decorator for thread-based tasks.

    Parameters
    ----------
    _func
        The function to be decorated.
    max_retries, optional
        Maximum number of retry attempts, by default 3
    sleep_time, optional
        Sleep time between retry attempts, by defaul 5

    Returns
    -------
    decorator
    """

    def decorator_retry(
        func: Callable, max_retries: int = max_retries, sleep_time: int = sleep_time
    ) -> Callable:
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs) -> Any:
            print(max_retries, sleep_time, args, kwargs)
            n_retries = 0
            while n_retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(
                        f"Failed to execute {func.__name__} on attempt {n_retries + 1}."
                    )
                    logging.warning(f"Exception: {e}.")
                    logging.warning(f"Retrying in {sleep_time} seconds.")

                n_retries += 1
                time.sleep(sleep_time)

            raise Exception(
                f"Failed to execute {func.__name__} after {n_retries} attempts."
            )

        return wrapper_retry

    if _func is None:
        # when using @retry(...), the decorated function
        # is not passed as the first argument, and _func is None
        return decorator_retry
    else:
        # when simply using @retry without arguments,
        # the decorated function is passed as the _func argument
        return decorator_retry(_func, max_retries=max_retries, sleep_time=sleep_time)

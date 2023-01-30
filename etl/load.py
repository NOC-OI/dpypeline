from typing import Callable
from .actions import subscribe_action, Action


def load_to_server(event, data, mapper, engine: str = "file", *args, **kwargs):
    # Send to server
    if engine.lower() == "file":
        with mapper.fs.open(f"{mapper.root}/{event.src_path.rsplit('/', 1)[-1]}", mode="wb") as fa:
            with open(event.src_path, mode="rb") as fb:
                fa.write(fb.read())

    elif engine.lower() == "zarr":
        import xarray as xr
        ds = xr.open_dataset(event.src_path).chunk("auto")
        tasks = ds.to_zarr(mapper, mode="a", compute=False, *args, **kwargs)
        tasks.compute()

    else:
        raise NotImplementedError(f"Engine '{engine}' is not implemented.")

    return data


def setup_load_action(load_func: Callable, *args, **kwargs) -> None:
    """
    Sets up a load action.

    Parameters
    ----------
    load_func
        Function that executes the load action.
    args
        Arguments to be passed to the function that executes the load action.
    kwargs
        Keyword arguments to be passed to the function that executes the load action.
    """
    subscribe_action(Action.LOAD, load_func, args, kwargs)


def setup_load_to_server(*args, **kwargs) -> None:
    setup_load_action(load_to_server, args, kwargs)

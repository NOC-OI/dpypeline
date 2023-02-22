"""Celery Xarray tasks."""
import numpy as np
import xarray as xr
from celery.utils.log import get_task_logger

from ..app import app

logger = get_task_logger(__name__)


@app.task(bind=True)
def open_dataset(self, filepath: str, *args, **kwargs):
    """
    Open and decode a dataset.

    Notes
    -----
    https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html
    """
    logger.info(f"Opening xarray data set at {filepath}")
    return xr.open_dataset(filepath, *args, **kwargs)


@app.task(bind=True)
def to_zarr(self, dataset: xr.Dataset, *args, **kwargs):
    """
    Write dataset contents to a zarr group.

    Notes
    -----
    https://docs.xarray.dev/en/stable/generated/xarray.to_zarr.html
    """
    return dataset.to_zarr("trial.zarr", *args, **kwargs)


@app.task(bind=True)
def clean_dataset(
    self,
    dataset: xr.Dataset,
    fill_value=np.nan,
    supression_value: float = 1e20,
    threshold: float = 1e-6,
    *args,
    **kwargs,
) -> str:
    """Clean a dataset."""
    fill_value = fill_value if fill_value is not None else np.NaN
    supression_value = supression_value if supression_value is not None else np.NaN

    dataset_tmp = dataset.copy(deep=True).load()
    # dataset_tmp = dataset_tmp.fillna(fill_value)
    # for var in ["sos"]:#dataset.keys():
    # dataset_tmp[var].values = xr.where(np.abs(dataset_tmp[var] - supression_value) < threshold, fill_value, dataset_tmp[var])
    # dataset_tmp[var].encoding["_FillValue"] = fill_value

    cache_dir = "../"
    cached_file = f"{cache_dir}/transformed_{dataset.encoding['source'].rsplit('/', 1)[-1].rsplit('.', 1)[-2]}.nc"
    dataset_tmp.to_netcdf(cached_file)
    del dataset_tmp
    print("CACHED_FILE", cached_file)
    return cached_file

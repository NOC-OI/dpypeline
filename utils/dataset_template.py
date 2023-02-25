"""Util function to create a dataset template."""
from typing import Iterable

import numpy as np
import xarray as xr


def create_template(
    ds: xr.Dataset,
    drop_dims: Iterable = None,
    drop_vars: Iterable = None,
    output_file: str = "template.nc",
) -> xr.Dataset:
    """
    Create a dataset template.

    Parameters
    ----------
    ds
        Dataset from which to create a template.
    drop_dims
        Dimensions to drop from the template.
    drop_vars
        Variables to drop from the template.
    output_file
        Name of the output NetCDF file.

    Returns
    -------
        Template dataset.
    """
    # Set values of all variables to np.nan
    template = xr.Dataset(ds.data_vars, ds.coords, ds.attrs)
    for var in template.keys():
        template[var].values = xr.where(np.isnan(template[var]), template[var], np.nan)
        template[var].encoding["_FillValue"] = np.nan
        template[var].encoding["missing_value"] = np.nan

    # Drop dimensions
    if drop_dims is not None:
        template = template.drop_dims(drop_dims)

    # Drop vars
    if drop_vars is not None:
        template = template.drop_vars(drop_vars)

    # Set all time coordinates to np.NaT
    for coord in template.coords.keys():
        if coord.startswith("time"):
            nan_array = np.empty(template[coord].shape)
            nan_array[:] = np.nan
            template[coord] = nan_array

    template.to_netcdf(output_file)

    return template


if __name__ == "__main__":
    ds = xr.open_dataset(
        "/home/joaomorado/git_repos/dpypeline/utils/ORCA0083-N06_1958m01T.nc"
    )
    template = create_template(ds, drop_vars=["time_instant", "time_centered"])

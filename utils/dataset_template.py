"""Util function to create a dataset template."""
import glob
from functools import partial
from typing import Iterable

import numpy as np
import xarray as xr

from dpypeline.filesystems.object_store import ObjectStoreS3


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


def create_template_new(
    files, store, coords_list, dims_list, vars_list, chunks, fill_val=np.nan
):
    """Create template."""

    def preprocess_template(coords_list, dims_list, vars_list, ds):
        """Preprocess template callback."""
        # Drop non-matching coords
        ds = ds.set_coords([coord for coord in coords_list if coord in ds.variables])
        ds = ds.reset_coords([coord for coord in ds.coords if coord not in coords_list])
        # Drop non-matching dims
        ds = ds.drop_dims([dim for dim in ds.dims if dim not in dims_list])
        # Drop non-matching vars
        ds = ds.drop_vars([var for var in ds.variables if var not in vars_list])

        return ds

    # Open multiple files as a single dataset
    ds = xr.open_mfdataset(
        files,
        preprocess=partial(preprocess_template, coords_list, dims_list, vars_list),
        combine="by_coords",
        compat="override",
        combine_attrs="override",
        data_vars="all",
        coords="all",
        join="outer",
        parallel=True,
    )

    # Fix the encoding of the variables
    for var in ds.variables:
        if "_FillValue" in ds[var].encoding:
            ds[var].encoding["_FillValue"] = np.nan
        if "missing_value" in ds[var].encoding:
            ds[var].encoding["missing_value"] = np.nan

    # Chunck the dataset
    ds = ds.chunk(chunks)
    ds.to_zarr(store, compute=False)

    # Create template
    ds.isel(time_counter=slice(0, 1)).to_zarr("template.zarr", compute=False)

    return True


if __name__ == "__main__":
    ds = xr.open_dataset(
        "/home/joaomorado/git_repos/dpypeline/utils/ORCA0083-N06_1958m01T.nc"
    )
    template = create_template(ds, drop_vars=["time_instant", "time_centered"])

    jasmin = ObjectStoreS3(
        anon=False, store_credentials_json="jasmin_object_store_credentials.json"
    )
    bucket = "joaomorado"
    # jasmin.mkdir(bucket)

    dims_list = ["time_counter", "deptht", "y", "x"]
    coords_list = ["time_counter", "deptht"]
    vars_list = [
        "deptht",
        "sst",
        "time_instant",
        "time_counter",
        "sss",
        "ssh",
        "time_centered",
        "potemp",
        "salin",
        "tossq",
        "zossq",
        "mldkz5",
        "mldr10_1",
        "wfo",
        "rsntds",
        "tohfls",
        "sosflxdo",
        "taum",
        "sowindsp",
        "soprecip",
        "e3t",
        "nav_lat",
        "nav_lon",
    ]

    files = glob.glob(
        "/gws/nopw/j04/nemo_vol1/public/runs/ORCA0083-N06/means/**/ORCA0083-N06_*m*T.nc"
    )
    chunks = {"x": 577, "y": 577, "time_counter": 1, "deptht": 5}

    store = jasmin.get_mapper(bucket + "/n06.zarr")
    # store = "trial.zarr"
    create_template_new(
        files, store, coords_list, dims_list, vars_list, chunks, fill_val=np.nan
    )

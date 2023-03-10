"""Tasks for use by thread-based data pipelines."""
import logging
import os
from difflib import SequenceMatcher
from typing import Any, Iterable, Sequence

import numpy as np
import xarray as xr

from dpypeline.etl_pipeline.decorators import retry


# @retry(max_retries=3, sleep_time=5)
def clean_dataset(
    dataset: xr.Dataset,
    fill_value=np.nan,
    missing_value: float = 1e20,
    threshold: float = 1e-6,
    write_file: bool = False,
    *args,
    **kwargs,
) -> xr.Dataset | str:
    """
    Clean a dataset.

    Set both the missings and fill values equal to the provided fill value.

    Notes
    -----
    If write_file is True, the name of the file is returned.
    If write_file is False, the cleaned dataset is returned.

    Parameters
    ----------
    dataset
        Dataset to be cleaned.
    fill_value, optional
        Fill value to be used, , by default np.nan
    missing_value, optional
        Missing value, by default 1e20
    threshold, optional
        Supression value threshold, by default 1e-6
    write_file, optional
        If True writes the cleaned dataset to a file, by default False

    Returns
    -------
        Name of the file if write_file is True, else the cleaned dataset.
    """
    fill_value = fill_value if fill_value is not None else np.NaN
    missing_value = missing_value if missing_value is not None else np.NaN

    for var in dataset.keys():
        tmp = dataset[var].load()

        try:
            tmp.values = tmp.fillna(fill_value)
            tmp.encoding["_FillValue"] = fill_value
        except TypeError:
            logging.warning(
                f"Cannot fill variable {var} with NA/NaN values. Skiping this step and leaving the _Fillvalue attribute unchanged."
            )

        try:
            tmp.values = xr.where(
                np.abs(tmp - missing_value) < threshold, fill_value, tmp
            )
            tmp.encoding["missing_value"] = fill_value
        except TypeError:
            logging.warning(
                f"Cannot change variable {var} missing values values. Skiping this step and leaving the missing_value attribute unchanged."
            )

    if write_file:
        cached_file = f"{os.environ['CACHE_DIR']}/transformed_{dataset.encoding['source'].rsplit('/', 1)[-1].rsplit('.', 1)[-2]}.nc"
        dataset.to_netcdf(cached_file)
        return cached_file

    return dataset


def to_zarr(dataset: xr.Dataset, *args, **kwargs) -> Any:
    """
    Xarray to_zarr wrapper.

    """
    if "region_dict" in kwargs:
        event = dataset.encoding["source"]
        idx = kwargs["region_dict"][event]
        region = {"time_counter": slice(idx, idx + 1)}
        del kwargs["region_dict"]
        print(region)
        exit()
    else:
        region = None

    try:
        return dataset.to_zarr(*args, region=region, **kwargs)
    except ValueError:
        new_kwargs = kwargs.copy()
        del new_kwargs["append_dim"]
        return dataset.to_zarr(*args, region=region, **new_kwargs)


def to_netcdf(dataset: xr.Dataset, *args, **kwargs):
    """to_netcdf wrapper."""
    dataset.to_netcdf(*args, **kwargs)


def clean_cache_dir(_):
    """Clean the cache directory."""
    import glob

    file_list = glob.glob(
        os.path.join(os.environ["CACHE_DIR"], "") + "*.nc", recursive=True
    )
    for file in file_list:
        try:
            os.remove(file)
        except OSError:
            logging.info(f"Failed to delete {file}")

    return


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

    template.to_netcdf(output_file)

    return template


def similar(a: Sequence, b: Sequence) -> float:
    """
    Return the similarity of two sequences.

    Parameters
    ----------
    a
        Sequence 1.
    b
        Seqence 2.

    Returns
    -------
        The similarity of two sequences.
    """
    return SequenceMatcher(None, a, b).ratio()


def create_reference_names_dict(ds: xr.Dataset) -> dict:
    """
    Create a dictionary of reference names.

    Notes
    -----
    long_name keys are mapped to variables names.

    Parameters
    ----------
    ds
        Dataset from which to create a reference names dictionary.

    Returns
    -------
        Reference names dictionary.
    """
    ref_names_dict = {}
    for var in ds.keys():
        ref_names_dict[ds[var].attrs["long_name"].lower()] = var

    return ref_names_dict


def rename_vars(ds: xr.Dataset, reference_names: dict):
    """
    Rename the names of variables in a dataset.

    Parameters
    ----------
    ds
        Dataset to fix.
    reference_names
        Dictionary of reference names.

    Returns
    -------
        Dataset with fixed names.
    """
    logging.info("Fixing variables' names")
    rename_dict = {}
    for var in ds.keys():
        found = False
        if "long_name" in ds[var].attrs.keys():
            long_name = ds[var].attrs["long_name"]
            for ref_var in reference_names.keys():
                similarity = similar(long_name.lower(), ref_var.lower())
                if similarity > 0.9:
                    logging.info("")
                    logging.info(
                        f"Renaming var: {long_name.lower()} = {ref_var.lower()} with similarity {similarity}."
                    )
                    logging.info(f"Renaming var: {var} -> {reference_names[ref_var]}")
                    rename_dict[var] = reference_names[ref_var]
                    found = True
                    break

            if not found:
                logging.info(f"{long_name} has no correspondence in reference names.")

        else:
            logging.info(f"{var} has no long_name attribute. Renaming is not possible.")

    ds = ds.rename_vars(rename_dict)

    return ds


def match_to_template(ds: xr.Dataset, template: xr.Dataset):
    """
    Match a dataset to a template.

    Parameters
    ----------
    ds
        Dataset to combine.
    template
        Template dataset.

    Returns
    -------
        Combined dataset.
    """
    # Match dataset to template
    # Reset non-matching coords to become variables
    ds = ds.reset_coords([coord for coord in ds.coords if coord not in template.coords])
    # Drop non-matching dims
    ds = ds.drop_dims([dim for dim in ds.dims if dim not in template.dims])
    # Drop non-matching vars
    ds = ds.drop_vars([var for var in ds.variables if var not in template.variables])

    # Set the correct time_counter
    template["time_counter"] = ds["time_counter"]

    # Store the source
    source = ds.encoding["source"]

    # Project ds onto template
    ds = template.combine_first(ds)

    # Recover the time counter
    ds.encoding["source"] = source

    return ds

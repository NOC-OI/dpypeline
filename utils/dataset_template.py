"""Util function to create a dataset template."""
import logging
from typing import Iterable, Sequence

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
    """
    for coord in template.coords.keys():
        if coord.startswith("time"):
            template [xr.where(template.coords[coord] == np.datetime64('NaT'), template.coords[coord], np.datetime64('NaT'))
    """
    template.to_netcdf(output_file)

    return template


from difflib import SequenceMatcher


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


def fix_name_vars(ds: xr.Dataset, reference_names: dict):
    """
    Fix the names of variables in a dataset.

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

    ds = ds.rename_vars(rename_dict)

    return ds


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    from IPython import embed

    # ds = xr.open_dataset("/home/joaomorado/git_repos/dpypeline/utils/ORCA0083-N06_1958m01T.nc")
    # template = create_template(ds, drop_vars=["time_instant", "time_centered"])

    template = xr.open_dataset("/home/joaomorado/git_repos/dpypeline/utils/template.nc")
    ds = xr.open_dataset(
        "/home/joaomorado/PycharmProjects/msm_project/nc_files/ORCA0083-N06_2015m01T.nc"
    )

    ds_new = fix_name_vars(ds, create_reference_names_dict(template))
    print(ds)

    print(ds_new)

    x = template.combine_first(ds_new)
    print(x)
    embed()

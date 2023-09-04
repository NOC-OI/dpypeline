"""Tasks used in the pipeline of example 1."""
import os

import pandas as pd


def read_csv(path: str) -> pd.DataFrame:
    """
    Read a csv file.

    Parameters
    ----------
    path
        Path to the csv file.

    Returns
    -------
    df
        Dataframe.
    """
    df = pd.read_csv(path)
    df.attrs["filename"] = path.rsplit("/", 1)[1]
    return df


def convert_fahrenheit_to_celsius(df: pd.DataFrame, col_temp: str) -> pd.DataFrame:
    """
    Convert the col_temp of a dataframe from Fahrenheit to Celsius.

    Parameters
    ----------
    df
        Dataframe to convert.
    col_temp
        Name of the column to convert.

    Returns
    -------
    df
        Dataframe with converted col_temp.
    """
    df[col_temp] = (df[col_temp] - 32) * 5.0 / 9.0
    return df


def write_csv(df: pd.DataFrame, prefix: str) -> None:
    """
    Write a dataframe to a csv file.

    Parameters
    ----------
    df
        Dataframe to write.
    prefix
        Path to write the csv file to.
    """
    directory_path = prefix.rsplit("/", 1)[0]
    os.makedirs(directory_path, exist_ok=True)
    df.to_csv(os.path.join(prefix, df.attrs["filename"]))

import pandas as pd


def calculate_speed_up(df: pd.DataFrame):
    """
    Calculate the speed-up for each dataset. Add a columns to the Dataframe.

    Parameters
    ----------
    df
        Pandas Dataframe.

    Returns
    -------
    df
        Pandas Dataframe with "Speed-up" column added.
    """
    # Calculate the average mean for the single-core runs
    datasets = df.drop_duplicates("Dataset")["Dataset"].tolist()

    # Speed-up
    df["Speed-up"] = ""
    for dataset in datasets:
        df.loc[df["Dataset"] == dataset, ["Speed-up"]] = df.query(
            f"Configuration == '1:1:1' & Dataset == '{dataset}'"
        ).mean()["Walltime"]

    df["Speed-up"] = df["Speed-up"] / df["Walltime"]

    return df

import matplotlib.pyplot as plt
import pandas
import seaborn as sns


def plot_barplot(
    ax: plt.axes,
    df: pandas.DataFrame,
    x: str = "Dataset",
    y: str = "Walltime",
    hue: str = "Number of cores",
    ylabel: str = "Walltime [s]",
    xlabel: str = "",
    log: bool = True,
    leg_title: str = "",
    pallette=sns.color_palette("deep"),
    error_kwargs: dict | None = None,
):
    """_summary_

    Parameters
    ----------
    ax
        _description_
    df
        Pandas DataFrame.
    x, optional
        _description_, by default "Dataset"
    y, optional
        _description_, by default "Walltime"
    hue, optional
        _description_, by default "Number of cores"
    ylabel, optional
        _description_, by default "Walltime [s]"
    xlabel, optional
        _description_, by default ""
    log, optional
        _description_, by default True
    leg_title, optional
        _description_, by default ""
    pallette, optional
        _description_, by default sns.color_palette("deep")
    error_kwargs, optional
        _description_, by default None

    Returns
    -------
        _description_
    """
    if error_kwargs is None:
        error_kwargs = {}

    sns.barplot(
        x=df[x], y=df[y], hue=df[hue], ax=ax, palette=pallette, log=log, **error_kwargs
    )

    # Grid
    ax.grid(visible=True, which="major", axis="y")

    # Label
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.xaxis.label.set_color("grey")
    ax.yaxis.label.set_color("grey")

    # Spines' visibility
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(True)
    ax.spines["left"].set_visible(True)

    # Spines' color
    ax.spines["top"].set_color("grey")
    ax.spines["right"].set_color("grey")
    ax.spines["bottom"].set_color("grey")
    ax.spines["left"].set_color("grey")

    # Ticks
    ax.tick_params(colors="grey", which="both")

    # Legend
    leg = ax.legend(frameon=False, labelcolor="grey", title=leg_title)
    plt.setp(leg.get_title(), color="grey")

    return ax


def plot_lineplot(
    ax: plt.axes,
    df: pandas.DataFrame,
    x: str = "Number of cores",
    y: str = "Speed-up",
    hue: str = "Dataset",
    ylabel: str = r"Speed-up$=t(1)\ /\ t(N_{cores})$",
    xlabel: str = "Number of cores",
    leg_title: str = "",
    pallette=sns.color_palette("deep"),
    lw: float = 1.5,
    alpha: float = 1,
    linestyle: str = "dashed",
    marker: str = "o",
    markersize: float = 7,
    error_kwargs: dict | None = None,
):
    if error_kwargs is None:
        error_kwargs = {}

    sns.lineplot(
        x=df[x],
        y=df[y],
        hue=df[hue],
        ax=ax,
        palette=pallette,
        lw=lw,
        alpha=alpha,
        linestyle=linestyle,
        marker=marker,
        markersize=markersize,
        **error_kwargs
    )

    # Grid
    ax.grid(visible=True, which="major", axis="both")

    # Label
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.xaxis.label.set_color("grey")
    ax.yaxis.label.set_color("grey")

    # Spines' visibility
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(True)
    ax.spines["left"].set_visible(True)

    # Spines' color
    ax.spines["top"].set_color("grey")
    ax.spines["right"].set_color("grey")
    ax.spines["bottom"].set_color("grey")
    ax.spines["left"].set_color("grey")

    # Ticks
    ax.tick_params(colors="grey", which="both")

    # Legend
    leg = ax.legend(frameon=False, labelcolor="grey", title=leg_title)
    plt.setp(leg.get_title(), color="grey")

    return ax

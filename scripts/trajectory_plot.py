"""
Create a trajectory plot for a section of time, overplots bow shock crossing intervals.
"""

import datetime as dt

import hermpy.boundary_crossings as boundaries
import hermpy.mag as mag
import hermpy.plotting_tools as hermplot
import hermpy.trajectory as traj
import matplotlib.pyplot as plt
import numpy as np
import spiceypy as spice
from matplotlib.collections import LineCollection


def main():
    colours = ["#648FFF", "#785EF0", "#DC267F", "#FE6100", "#FFB000"]

    spice.furnsh("/home/daraghhollman/Main/SPICE/messenger/metakernel_messenger.txt")

    # Load the crossing intervals
    crossings = boundaries.Load_Crossings(
        "/home/daraghhollman/Main/Work/mercury/DataSets/philpott_2020_reformatted.csv"
    )

    # Case 1
    # start = dt.datetime(year=2012, month=4, day=22, hour=3, minute=0)
    # end = dt.datetime(year=2012, month=4, day=23, hour=19, minute=0)

    # Case 2
    # start = dt.datetime(year=2012, month=5, day=28, hour=19, minute=30)
    # end = dt.datetime(year=2012, month=5, day=30, hour=10, minute=20)

    # Case 3
    # start = dt.datetime(year=2013, month=8, day=20, hour=12, minute=0)
    # end = dt.datetime(year=2013, month=8, day=21, hour=15, minute=15)

    # Case 4
    start = dt.datetime(year=2014, month=10, day=15, hour=16, minute=0)
    end = dt.datetime(year=2014, month=10, day=17, hour=4, minute=0)

    fig, axes = plt.subplots(1, 2)

    # Plot boundary intervals

    # Get bow shocks crossings within start and end
    bow_shocks = crossings.loc[
        (
            (crossings["start"].between(start, end))
            & (crossings["end"].between(start, end))
        )
        & ((crossings["type"] == "BS_OUT") | (crossings["type"] == "BS_IN"))
    ]

    # Iterate through rows and plot trajectory for data within
    i = 0
    labels = ["Onset Bow Shock", "Next Bow Shock"]
    for _, row in bow_shocks.iterrows():

        # Load trajectory
        interval_positions = (
            traj.Get_Trajectory(
                "Messenger", [row["start"], row["end"]], frame="MSM", aberrate=True
            )
            / 2439.7
        )

        axes[0].plot(
            interval_positions[:, 0],
            interval_positions[:, 1],
            c=colours[i+1],
            lw=3,
            zorder=15,
            label=labels[i]
        )
        axes[1].plot(
            interval_positions[:, 0],
            interval_positions[:, 2],
            c=colours[i+1],
            lw=3,
            zorder=15,
            label=labels[i]
        )

        i += 1

    # Plot trajectory
    frame = "MSM"

    dates = [start, end]

    positions = traj.Get_Trajectory("Messenger", dates, frame=frame, aberrate=True)

    # Convert from km to Mercury radii
    positions /= 2439.7

    colored_line(
        positions[:, 0],
        positions[:, 1],
        0,
        axes[0],
        color="black",
        lw=3,
        zorder=10,
        alpha=0.2,
    )
    colored_line(
        positions[:, 0],
        positions[:, 2],
        0,
        axes[1],
        color="black",
        lw=3,
        zorder=10,
        alpha=0.2,
    )

    planes = ["xy", "xz"]
    shaded = ["left", "left"]
    for i, ax in enumerate(axes):
        hermplot.Plot_Mercury(
            ax, shaded_hemisphere=shaded[i], plane=planes[i], frame=frame
        )
        hermplot.Add_Labels(ax, planes[i], frame=frame, aberrate=True)
        hermplot.Plot_Magnetospheric_Boundaries(ax, plane=planes[i], add_legend=True)
        hermplot.Square_Axes(ax, 6)

    
    # icme_arrival = dt.datetime(year=2013, month=8, day=20, hour=12, minute=41)
    # icme_position = traj.Get_Trajectory("Messenger", [icme_arrival, icme_arrival], frame="MSM", aberrate=True) / 2439.7
    # axes[0].scatter(icme_position[:, 0], icme_position[:, 1], c=colours[-1])
    # axes[1].scatter(icme_position[:, 0], icme_position[:, 2], c=colours[-1], label="ICME Arrival")


    axes[1].legend()

    plt.show()


def colored_line(x, y, c, ax, **lc_kwargs):
    """
    Plot a line with a color specified along the line by a third value.

    It does this by creating a collection of line segments. Each line segment is
    made up of two straight lines each connecting the current (x, y) point to the
    midpoints of the lines connecting the current point with its two neighbors.
    This creates a smooth line with no gaps between the line segments.

    Parameters
    ----------
    x, y : array-like
        The horizontal and vertical coordinates of the data points.
    c : array-like
        The color values, which should be the same size as x and y.
    ax : Axes
        Axis object on which to plot the colored line.
    **lc_kwargs
        Any additional arguments to pass to matplotlib.collections.LineCollection
        constructor. This should not include the array keyword argument because
        that is set to the color argument. If provided, it will be overridden.

    Returns
    -------
    matplotlib.collections.LineCollection
        The generated line collection representing the colored line.
    """
    if "array" in lc_kwargs:
        warnings.warn('The provided "array" keyword argument will be overridden')

    # Default the capstyle to butt so that the line segments smoothly line up
    default_kwargs = {"capstyle": "butt"}
    default_kwargs.update(lc_kwargs)

    # Compute the midpoints of the line segments. Include the first and last points
    # twice so we don't need any special syntax later to handle them.
    x = np.asarray(x)
    y = np.asarray(y)

    x_midpts = np.hstack((x[0], 0.5 * (x[1:] + x[:-1]), x[-1]))
    y_midpts = np.hstack((y[0], 0.5 * (y[1:] + y[:-1]), y[-1]))

    # Determine the start, middle, and end coordinate pair of each line segment.
    # Use the reshape to add an extra dimension so each pair of points is in its
    # own list. Then concatenate them to create:
    # [
    #   [(x1_start, y1_start), (x1_mid, y1_mid), (x1_end, y1_end)],
    #   [(x2_start, y2_start), (x2_mid, y2_mid), (x2_end, y2_end)],
    #   ...
    # ]
    coord_start = np.column_stack((x_midpts[:-1], y_midpts[:-1]))[:, np.newaxis, :]
    coord_mid = np.column_stack((x, y))[:, np.newaxis, :]
    coord_end = np.column_stack((x_midpts[1:], y_midpts[1:]))[:, np.newaxis, :]
    segments = np.concatenate((coord_start, coord_mid, coord_end), axis=1)

    lc = LineCollection(segments, **default_kwargs)
    lc.set_array(c)  # set the colors of each segment

    return ax.add_collection(lc)


if __name__ == "__main__":
    main()

"""
For particularly low Alfenic Mach numbers, we expect Mercury's bow shock to disappear. Bowers 2024 [in prep].
We know the period of MESSENGER's orbit, and can hence determine if there are times in the crossings file where we expect a crossing, but there isn't.
We can do this by determining the time between each crossing, and the next, and seeing if there are values outside of ~12 hours, and ~8 hours.
Particularly, we are looking for larger values.
"""
import datetime as dt

import hermpy.trajectory as traj
import hermpy.boundary_crossings as boundaries
import hermpy.mag as mag
import hermpy.plotting_tools as hermplot
import matplotlib.pyplot as plt
import numpy as np
import spiceypy as spice

colours = ["#648FFF", "#785EF0", "#DC267F", "#FE6100", "#FFB000"]

spice.furnsh("/home/daraghhollman/Main/SPICE/messenger/metakernel_messenger.txt")

# Load the crossing intervals
crossings = boundaries.Load_Crossings(
    "/home/daraghhollman/Main/Work/mercury/philpott_2020_reformatted.csv"
)

crossing_types = ["BS_IN", "BS_OUT"]

fig, axes = plt.subplots(2, 1, sharex=True)
axes = axes.flatten()

events: list[dict] = []

for i, crossing_type in enumerate(crossing_types):
    # Filter crossings to only that crossing type
    filtered_crossings = crossings.loc[crossings["type"] == crossing_type]
    filtered_crossings.reset_index(drop=True, inplace=True)

    # Itterate through the resulting list and record time between crossings
    # of the same type.
    crossing_periods = []

    for j, row in filtered_crossings.iterrows():

        if j == len(filtered_crossings) - 1:
            continue

        next_row = filtered_crossings.iloc[j + 1]

        time_difference = (next_row["start"] - row["start"]).total_seconds() / 3600

        crossing_periods.append(time_difference)

        if time_difference > 20:
            events.append(
                {
                    "first_crossing": row,
                    "next_crossing": next_row,
                    "dt": time_difference,
                }
            )

    hourly_bins = np.arange(0, np.ceil(np.max(crossing_periods)) + 1, 1)
    hist_data, _, _ = axes[i].hist(crossing_periods, bins=hourly_bins)

    axes[i].set_title(crossing_type)
    axes[i].margins(0)
    axes[i].set_yscale("log")
    axes[i].set_ylim(0.9, np.max(hist_data))

axes[1].set_xlabel("Time Between Crossing Interval Start Times")

plt.show()


for i, event in enumerate(events):

    print(f"Plotting event {i}/{len(events)-1}")

    try:
        data = mag.Load_Between_Dates(
            "/home/daraghhollman/Main/data/mercury/messenger/mag/avg_1_second/",
            event["first_crossing"]["start"],
            event["next_crossing"]["end"],
            strip=True,
        )
    except:
        continue

    print(event["first_crossing"]["start"])
    print(event["next_crossing"]["end"])
    print(event["dt"])

    fig = plt.figure()

    mag_axis = plt.subplot2grid((2, 2), (1, 0), colspan=2)
    xy_axis = plt.subplot2grid((2, 2), (0, 0), colspan=1)
    xz_axis = plt.subplot2grid((2, 2), (0, 1), colspan=1)


    mag_axis.plot(data["date"], data["mag_total"], color="black", lw=1, label="|B|")
    mag_axis.plot(
        data["date"], data["mag_x"], color=colours[2], lw=0.8, alpha=0.8, label="Bx"
    )
    mag_axis.plot(
        data["date"], data["mag_y"], color=colours[0], lw=0.8, alpha=0.8, label="By"
    )
    mag_axis.plot(
        data["date"], data["mag_z"], color=colours[-1], lw=0.8, alpha=0.8, label="Bz"
    )

    mag_leg = mag_axis.legend(
        bbox_to_anchor=(0.5, 1.1), loc="center", ncol=4, borderaxespad=0.5
    )

    # set the linewidth of each legend object
    for legobj in mag_leg.legend_handles:
        legobj.set_linewidth(2.0)

    # Add the boundary crossings
    boundaries.Plot_Crossing_Intervals(
        mag_axis,
        data["date"].iloc[0],
        data["date"].iloc[-1],
        crossings,
        color=colours[3],
        lw=1.5,
    )

    # Format the panels
    mag_axis.set_xmargin(0)
    mag_axis.set_ylabel("Magnetic Field Strength [nT]")
    hermplot.Add_Tick_Ephemeris(mag_axis)

    # Plot Trajectories!
    frame = "MSM"

    start = data["date"].iloc[0]
    end = data["date"].iloc[-1]

    dates = [start, end]

    positions = traj.Get_Trajectory("Messenger", dates, frame=frame, aberrate=True)

    # Convert from km to Mercury radii
    positions /= 2439.7


    xy_axis.plot(
        positions[:, 0],
        positions[:, 1],
        color="grey",
        lw=1,
        zorder=10,
        label="MESSENGER Trajectory",
    )
    xz_axis.plot(
        positions[:, 0],
        positions[:, 2],
        color="grey",
        lw=1,
        zorder=10,
    )

    planes = ["xy", "xz"]
    shaded = ["left", "left"]
    for i, ax in enumerate([xy_axis, xz_axis]):
        hermplot.Plot_Mercury(
            ax, shaded_hemisphere=shaded[i], plane=planes[i], frame=frame
        )
        hermplot.Add_Labels(ax, planes[i], frame=frame, aberrate=True)
        hermplot.Plot_Magnetospheric_Boundaries(ax, plane=planes[i], add_legend=True)
        hermplot.Square_Axes(ax, 6)

    plt.show()

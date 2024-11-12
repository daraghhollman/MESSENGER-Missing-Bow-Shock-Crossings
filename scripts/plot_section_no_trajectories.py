"""
Plot a mag planel and trajectory panels for a given time. Overplot the boundary crossings on the mag panel and the trajectory.
"""

import datetime as dt

import matplotlib.pyplot as plt
import numpy as np
import hermpy.mag as mag
import hermpy.trajectory as traj
import hermpy.fips as fips
import hermpy.boundary_crossings as boundaries
import hermpy.plotting_tools as hermplot
import spiceypy as spice

colours = ["#648FFF", "#785EF0", "#DC267F", "#FE6100", "#FFB000"]

spice.furnsh("/home/daraghhollman/Main/SPICE/messenger/metakernel_messenger.txt")

# Load the crossing intervals
crossings = boundaries.Load_Crossings(
    "/home/daraghhollman/Main/Work/mercury/philpott_2020_reformatted.csv"
)

start = dt.datetime(year=2013, month=8, day=15, hour=0, minute=0)
end = dt.datetime(year=2013, month=8, day=23, hour=23, minute=59)
    
data = mag.Load_Between_Dates(
    "/home/daraghhollman/Main/data/mercury/messenger/mag/avg_1_second/",
    start,
    end,
    strip=True,
)

fig, (mag_axis, fips_axis) = plt.subplots(2, 1, sharex=True)


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
    crossings.loc[ (crossings["type"] == "MP_IN") | (crossings["type"] == "MP_OUT") ],
    color=colours[3],
    lw=1.5,
    label=False
)
boundaries.Plot_Crossing_Intervals(
    mag_axis,
    data["date"].iloc[0],
    data["date"].iloc[-1],
    crossings.loc[ (crossings["type"] == "BS_IN") | (crossings["type"] == "BS_OUT") ],
    color="black",
    lw=1.5,
    label=False
)

# Format the panels
mag_axis.set_xmargin(0)
mag_axis.set_ylabel("Magnetic Field Strength [nT]")

# FIPS!

fips_data = fips.Load_Between_Dates("/home/daraghhollman/Main/data/mercury/messenger/FIPS/", start, end, strip=True)

# We transpose to place the time axis along x
protons = np.transpose(fips_data["proton_energies"])

# When using flat shading for pcolormesh, we require that the
# arrays defining the axes be one larger than the data. This is
# to define the last edge.
# We can achieve this simply by removing the last column of the
# data.
protons = np.delete(protons, -1, 1)

fips_calibration = fips.Get_Calibration()

cmap = "plasma"
protons_mesh = fips_axis.pcolormesh(
    fips_data["dates"], fips_calibration, protons, norm="log", cmap=cmap
)

colorbar_label = "Diff. Energy Flux\n[(keV/e)$^{-1}$ sec$^{-1}$ cm$^{-2}$]"
# plt.colorbar(protons_mesh, ax=fips_axis, label="Proton " + colorbar_label)

hermplot.Add_Tick_Ephemeris(fips_axis)

fips_axis.set_ylabel("E/Q [keV/Q]")
fips_axis.set_yscale("log")

plt.show()

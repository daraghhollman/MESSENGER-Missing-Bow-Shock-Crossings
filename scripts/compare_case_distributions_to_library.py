"""
We have a library of solar wind and magnetosheath distributions which we can compare to the distributions within each case.

Between two times (case start, and end), we compile the distribution of data between magnetopause crossings and overlay with library distributions.
"""

import datetime as dt
import multiprocessing

import hermpy.boundary_crossings as boundaries
import hermpy.mag as mag
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats
from tqdm import tqdm

colours = ["#648FFF", "#785EF0", "#DC267F", "#FE6100", "#FFB000"]


# Load the crossing intervals
crossings = boundaries.Load_Crossings(
    "/home/daraghhollman/Main/Work/mercury/DataSets/philpott_2020_reformatted.csv"
)

magnetosheath_samples = pd.read_csv(
    "/home/daraghhollman/Main/Work/mercury/DataSets/magnetosheath_sample_10_mins.csv"
)
solar_wind_samples = pd.read_csv(
    "/home/daraghhollman/Main/Work/mercury/DataSets/solar_wind_sample_10_mins.csv"
)
for samples_data_set in [magnetosheath_samples, solar_wind_samples]:

    # Fix loading issues to do with an element being a series itself
    samples_data_set["|B|"] = samples_data_set["|B|"].apply(
        lambda x: list(map(float, x.strip("[]").split(",")))
    )
    samples_data_set["B_x"] = samples_data_set["B_x"].apply(
        lambda x: list(map(float, x.strip("[]").split(",")))
    )
    samples_data_set["B_y"] = samples_data_set["B_y"].apply(
        lambda x: list(map(float, x.strip("[]").split(",")))
    )
    samples_data_set["B_z"] = samples_data_set["B_z"].apply(
        lambda x: list(map(float, x.strip("[]").split(",")))
    )


# Load case data
start = dt.datetime(year=2013, month=8, day=15, hour=19, minute=0)
end = dt.datetime(year=2013, month=8, day=17, hour=5, minute=0)

crossings_to_consider = crossings.loc[
    (crossings["start"].between(start, end)) & (crossings["type"].str.contains("MP"))
].reset_index()

data_to_concatanate = []


def Get_Data_Between_MP(input):

    i, crossing = input

    next_crossing = crossings_to_consider.iloc[i + 1]

    if crossing["type"] != "MP_OUT":
        return None

    # If outbound, mark as start time
    data_slice_start = crossing["end"]

    # The next crossing will be the inbound
    data_slice_end = next_crossing["start"]

    # Load data slice
    return mag.Load_Between_Dates(
        "/home/daraghhollman/Main/data/mercury/messenger/mag/avg_1_second/",
        data_slice_start,
        data_slice_end,
        strip=True,
    )


cpus_available = multiprocessing.cpu_count()
with multiprocessing.Pool(cpus_available - 1) as pool:

    generator = pool.imap(
        Get_Data_Between_MP,
        [input for input in crossings_to_consider.iloc[:-1].iterrows()],
    )

    for element in tqdm(generator, total=len(crossings_to_consider) - 1):

        if element is not None:
            data_to_concatanate.append(element)


data = pd.concat(data_to_concatanate)

# Remove outliers
data = data[(scipy.stats.zscore(data.select_dtypes(include="float64")) < 3).all(axis=1)]

# PLOTTING

fig, axes = plt.subplots(4, 2, sharey="row", sharex="row")
"""
Two columns:
    1. Plot the distribution of the 4 components for the event
    2. Plot the distributions of both the solar wind, and the magnetosheath
       separately to compare.
"""

ks_test_samples_data = []
ks_test_samples_solar_wind = []
ks_test_samples_magnetosheath = []

binsize = 10
components = ["mag_total", "mag_x", "mag_y", "mag_z"]
labels = ["|B|", "B$_x$", "B$_y$", "B$_z$"]
for ax, component, colour, label in zip(axes[:, 0], components, colours, labels):

    bins = np.arange(np.min(data[component]), np.max(data[component]), binsize)

    ax.hist(
        data[component],
        bins=bins,
        orientation="horizontal",
        color="black",
        density=True,
    )

    ks_test_samples_data.append(data[component])

    ax.set_ylabel(label)


components = ["|B|", "B_x", "B_y", "B_z"]
for ax, component in zip(axes[:, 1], components):

    solar_wind_data = np.array(solar_wind_samples[component].explode().tolist())
    magnetosheath_data = np.array(magnetosheath_samples[component].explode().tolist())

    solar_wind_data = solar_wind_data[(np.abs(scipy.stats.zscore(solar_wind_data)) < 3)]
    magnetosheath_data = magnetosheath_data[
        (np.abs(scipy.stats.zscore(magnetosheath_data)) < 3)
    ]

    bins = np.arange(
        np.min(solar_wind_data),
        np.max(solar_wind_data),
        binsize,
    )
    ax.hist(
        solar_wind_data,
        bins=bins,
        orientation="horizontal",
        color="cornflowerblue",
        density=True,
        label="Solar Wind",
        alpha=0.8,
    )

    bins = np.arange(
        np.min(magnetosheath_data),
        np.max(magnetosheath_data),
        binsize,
    )
    ax.hist(
        magnetosheath_data,
        bins=bins,
        orientation="horizontal",
        color="indianred",
        density=True,
        label="Magnetosheath",
        alpha=0.8,
    )

    ks_test_samples_solar_wind.append(solar_wind_data)
    ks_test_samples_magnetosheath.append(magnetosheath_data)


# Determine T test
for i in range(len(ks_test_samples_data)):

    data_solar_wind_ks_test = scipy.stats.kstest(
        ks_test_samples_data[i], ks_test_samples_solar_wind[i]
    )
    data_magnetosheath_ks_test = scipy.stats.kstest(
        ks_test_samples_data[i], ks_test_samples_magnetosheath[i]
    )

    axes[:, 0][i].annotate(
        f"p-values:\n Solar Wind: {data_solar_wind_ks_test.pvalue:.3f}\n Magnetosheath: {data_magnetosheath_ks_test.pvalue:.3f}",
        xy=(1, 1),
        xycoords="axes fraction",
        size=10,
        ha="right",
        va="top",
        bbox=dict(boxstyle="round", fc="w"),
    )

axes[0, 1].legend()

plt.show()

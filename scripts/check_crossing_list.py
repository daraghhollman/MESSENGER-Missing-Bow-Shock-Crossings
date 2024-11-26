"""
We want to check the Sun / Philpott crossing interval list for missing crossings due to (i) something physical, (ii) data gaps, (iii) mislabelling / typos
"""

from hermpy.utils import User
import hermpy.boundary_crossings as boundaries


def main():
    # First we load the crossing list
    crossings = boundaries.Load_Crossings(User.CROSSING_LISTS["Philpott"])

    # Iterate through the crossings list
    missing_crossing_count = 0
    missing_crossing_indices = []
    missing_crossings = []

    for i, current_crossing in crossings.iterrows():
        assert type(i) == int

        try:
            next_crossing = crossings.iloc[i + 1]

        except:
            # We skip the last crossing as there is no following crossing
            break

        # Check what the next crossing should be
        expected_next_crossing_type = Expected_Next_Crossing_Type(current_crossing["Type"])

        if next_crossing["Type"] is not expected_next_crossing_type:
            # Next crossing is not what is expected!
            # Record as a missing crossing
            missing_crossing_count += 1
            missing_crossing_indices.append(i)
            missing_crossings.append(expected_next_crossing_type)

        else:
            continue


    print(f"# of missing crossings per type")
    print(f"BS_IN: {missing_crossings.count('BS_IN')}")
    print(f"BS_OUT: {missing_crossings.count('BS_OUT')}")
    print(f"MP_IN: {missing_crossings.count('MP_IN')}")
    print(f"MP_OUT: {missing_crossings.count('MP_OUT')}")
    print("")
    print(f"{missing_crossing_count} missing crossings total found subsequent to these boundary crossings:")
    print(crossings[["Type", "Start Time", "End Time"]].iloc[missing_crossing_indices])



def Expected_Next_Crossing_Type(current_crossing_type):
    """
    For an input crossing type, output the expected next type
    """

    match current_crossing_type:
        case "BS_IN":
            next_crossing_type = "MP_IN"

        case "MP_IN":
            next_crossing_type = "MP_OUT"

        case "MP_OUT":
            next_crossing_type = "BS_OUT"

        case "BS_OUT":
            next_crossing_type = "BS_IN"

        case _:
            # Check for typos
            raise ValueError(f"Unknown current crossing type: {current_crossing_type}")

    return next_crossing_type


if __name__ == "__main__":
    main()

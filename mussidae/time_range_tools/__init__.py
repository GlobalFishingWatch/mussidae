from .create_fishing_nonfishing_ranges import points_from_path
from .create_fishing_nonfishing_ranges import dedup_and_sort_points
from .create_fishing_nonfishing_ranges import fuzzy_delta
from .create_fishing_nonfishing_ranges import ranges_from_points
from .create_fishing_nonfishing_ranges import ranges_from_paths
from .merge_ranges_with_tracks 

def write_ranges(ranges, path):
    with open(path, "w") as f:
        f.write("mmsi,start_time,end_time,is_fishing\n")
        for row in ranges:
            f.write("{}\n".format(','.join(str(x) for x in row)))


def write_recarray(recarry, path):
    with open(path, "w") as f:
        f.write(','.join(str(x) for x in recarry.dtype.names) + '\n')
        for row in recarry:
            f.write(','.join(str(x) for x in row) + '\n')

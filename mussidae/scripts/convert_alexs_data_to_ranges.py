"""Convert Alex's crowd sourced data to fishing / nonfishing ranges


"""
from __future__ import division
import datetime
import csv
import pytz
from mussidae import time_range_tools as trtools
import numpy as np


# Using a full hour for the time range would result in duplicate point issues at 
# at the boundaries
almost_one_hour = datetime.timedelta(hours=1) - datetime.timedelta(milliseconds=1)

fishing_map = {
    "" : False,
    "Not fishing" : False,
    "Longliner" : True,
    "Purse seine" : True
}


def extract_points(path):

    with open(path) as f:
        for row in csv.DictReader(f, skipinitialspace=True):
            if row['raw_point_count'] == 0:
                # Skip any hours with no points in them
                continue
            mmsi = row['mmsi'].strip()
            start_txt = row['start_hour_ms'].strip()
            start_time = datetime.datetime.utcfromtimestamp(float(start_txt) / 1000).replace(tzinfo=pytz.utc)
            stop_time = start_time + almost_one_hour
            is_fishing = fishing_map[row['classification'].strip()]
            yield trtools.Point(mmsi, start_time, is_fishing)
            yield trtools.Point(mmsi, stop_time, is_fishing)



if __name__ == "__main__":
    with open("data-precursors/time-range-sources/non-public-sources/SALT") as f:
        salt = f.read().strip()
    np.random.seed(hash(salt) % 4294967295)
    raw_points = extract_points("data-precursors/time-range-sources/alexCrowdSourcedResults.csv")
    points = trtools.dedup_and_sort_points(raw_points)
    ranges_by_mmsi = trtools.ranges_from_points(points)
    ranges = trtools.anonymize_ranges(ranges_by_mmsi, salt)
    trtools.write_ranges(sorted(ranges), "data/time-ranges/alex_crowd_sourced.csv")

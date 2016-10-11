"""Extract fishing / non fishing ranges from Alex's crowd sourced data


"""
from __future__ import division
import datetime
import csv
import pytz
from mussidae import time_range_tools as trtools


# Using a full hour for the time range would result in duplicate point issues at 
# at the boundaries
almost_one_hour = datetime.timedelta(minutes=59)

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
            is_fishing = row['classification'].strip() not in ("", "Not fishing")
            assert not is_fishing or row['classification'].strip() in ["Longliner", "Purse seine"] 
            yield trtools.Point(mmsi, start_time, is_fishing)
            yield trtools.Point(mmsi, stop_time, is_fishing)




if __name__ == "__main__":
    raw_points = extract_points("data/time-range-sources/alexCrowdSourcedResults.csv")
    points = trtools.dedup_and_sort_points(raw_points)
    ranges = trtools.ranges_from_points(points)
    trtools.write_ranges(ranges, "data/time-ranges/alex_crowd_sourced.csv")

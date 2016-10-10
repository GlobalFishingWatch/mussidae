from __future__ import print_function
from __future__ import division
import numpy as np
import csv
import datetime
import pytz
import logging
from collections import namedtuple

Point = namedtuple("Points", "mmsi timestamp is_fishing")

Range = namedtuple("Range", "mmsi start_time stop_time is_fishing")


def points_from_path(path, dialect):
    """Read csv at path and convert to series of `Points`

    Each point contains an MMSI, time

    Args:
        path: str

        dialect: dict
            dictionary of getter functions for 'mmsi', 'timestamp'
            and 'is_fishing'

    Yields:
        Point

    """
    with open(path) as f:
        for row in csv.DictReader(f):
            yield Point(
                dialect.mmsi(row), dialect.timestamp(row),
                dialect.is_fishing(row))


def dedup_and_sort_points(points):
    """remove duplicate Points and sort the results by MMSI and timestamp

    Args:
        points: iter or Point

    Yields:
        Point
    """
    # Can't use points.sort, since points may be itereable.
    points = sorted(points)
    if not points:
        return
    dedupped = []
    last_key = (None, None)
    last_fishing = None
    last_item = None
    for pt in points:
        key = (pt.mmsi, pt.timestamp)
        fishing = None if pt.is_fishing in (-1, 2) else pt.is_fishing
        if key == last_key:
            if fishing != last_fishing:
                last_item = Point(pt.mmsi, pt.timestamp, None)
        else:
            if last_item is not None:
                yield last_item
            last_item = Point(pt.mmsi, pt.timestamp, fishing)
            last_key = key
            last_fishing = fishing
    yield last_item

# Never fuzz range edges more than this amount
MAX_TIME_DELTA = 10 * 60


def fuzzy_delta(t1, t0, in_same_mmsi):
    if in_same_mmsi:
        # Divide the range between this point and the next point by two
        # so that we can use simple, independent fuzzing without having
        # to worry about overlapping ranges.
        dt = min((t1 - t0).total_seconds() // 2, MAX_TIME_DELTA)
    else:
        dt = MAX_TIME_DELTA
    return datetime.timedelta(seconds=np.random.randint(dt))


def ranges_from_points(points):
    """create Ranges from Points

    Args:
        points: iter of Point

    Yields:
        Range

    """
    points = dedup_and_sort_points(points)
    current_state = None
    current_mmsi = None
    last_time = None
    ranges = []
    for mmsi, time, state in points:
        if mmsi != current_mmsi or state != current_state:
            if current_state is not None:
                range_end = last_time + fuzzy_delta(time, last_time, mmsi ==
                                                    current_mmsi)
                yield Range(current_mmsi, range_start.isoformat(),
                            range_end.isoformat(), current_state)
            current_state = state
            range_start = time - fuzzy_delta(time, last_time, mmsi ==
                                             current_mmsi)
            current_mmsi = mmsi
        last_time = time
    if current_state is not None:
        yield Range(current_mmsi, range_start.isoformat(),
                    last_time.isoformat(), current_state)


def ranges_from_paths(paths, dialect):
    """Load points from paths and turn into ranges

    Args:
        paths: iter of str
            paths to csv file containing AIS points. 
        dialect: dict
            dict of getters that convert 


    """
    for pth in paths:
        logging.info("converting {}".format(pth))
        try:
            points = points_from_path(pth, dialect)
            for rng in ranges_from_points(points):
                yield rng
        except StandardError as err:
            logging.warning("conversion failed for {}".format(pth))
            logging.warning(repr(err))

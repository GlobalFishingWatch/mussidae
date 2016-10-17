from __future__ import division
from __future__ import print_function
import os
import numpy as np
from numpy.lib.recfunctions import append_fields
import datetime
import pytz
import dateutil.parser
import logging
import csv
import bqtools
import mussidae.time_range_tools as trtools
from mussidae.time_range_tools import false_positives
from collections import defaultdict
import itertools as it


def is_sorted(x):
    last = x[0]
    for this in x[1:]:
        if this < last:
            return False
        last = this
    return True


def create_fishing_series(times, ranges):
    """

    Create fishing series based on ranges. MMSI must 
    all match.

    Parameters
    ==========
    times : sequence of datetime
        Sequence must be sorted

    ranges: sequence of Ranges

    Returns
    =======
    sequence of [0-1] or -1 for don't know
        whether the vessel is fishing at given point or -1 for
        don't know

    """
    if not is_sorted(times):
        raise ValueError("times must be sorted")
    # Initialize is_fishing to -1 (don't know)
    is_fishing = np.empty([len(times)], dtype=float)
    is_fishing.fill(-1)
    #
    last_mmsi = None
    for mmsi, start, end, state in ranges:
        if last_mmsi is not None and mmsi != last_mmsi:
            raise ValueError("mmsi must all match")
        last_mmsi = mmsi
        if state not in [-1, 2]:
            i0 = np.searchsorted(times, start, side="left")
            i1 = np.searchsorted(times, end, side="right")
            is_fishing[i0:i1] = state
    #
    return is_fishing
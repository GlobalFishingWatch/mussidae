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
from mussidae.time_range_tools.create_fishing_series import create_fishing_series
from collections import defaultdict
import itertools as it


proj_id = 'world-fishing-827'

fields = ["mmsi",
  "timestamp",
  # "seg_id",  # TODO[bitsofbits] look into what the best pipeline to pull this stuff from is.
  # "distance_from_shore", # TODO[bitsofbits]: these fields not present for all dates. investigate
  # "distance_from_port",
  "speed",
  "course",
  "lat",
  "lon"]


query_template = """
SELECT
    {fields}
FROM
   TABLE_DATE_RANGE([pipeline_normalize.],
                    TIMESTAMP('{start.year}-{start.month}-{start.day}'),
                    TIMESTAMP('{stop.year}-{stop.month}-{stop.day}'))
WHERE
  (lat is not null) AND (lon is not null) AND
  (mmsi in ({mmsi}))

LIMIT  {limit};
"""


def create_query(mmsi, start, stop, limit=10000000):
    fields_str = ",\n".join(fields)
    return query_template.format(
        start=start, stop=stop, limit=limit, fields=fields_str, mmsi=mmsi)


def group_ranges_by_mmsi(ranges):
    """
    Parameters
    ==========          
    ranges: sequence of Ranges

    Returns
    =======
    dict mapping mmsi to sets of Ranges

    """
    grouped = defaultdict(set)
    for rng in ranges:
        grouped[rng.mmsi].add(rng)
    return grouped


def consolidate_ranges(grouped_ranges):
    """

    Parameters
    ==========          
    grouped_ranges: dict
        maps mmsi to of iterable of Ranges (see `group_ranges_by_mmsi`)

    Returns
    =======
    dict mapping mmsi to Ranges

    """
    consolidated = {}
    for group in grouped_ranges.values():
        for rng in group:
            if rng.mmsi in consolidated:
                _, t0, t1, _ = consolidated[rng.mmsi]
                t0 = min(t0, rng.start_time)
                t1 = max(t1, rng.stop_time)
            else:
                t0 = rng.start_time
                t1 = rng.stop_time
            consolidated[rng.mmsi] = trtools.Range(rng.mmsi, t0, t1, None)
    return consolidated.values()



def _yield_ranges_within_dates(start, stop, ranges):
    for (mmsi, t0, t1, _) in ranges:
        t0, t1 = [x.date() for x in [t0, t1]]
        if t0 > start or t1 < stop:
            continue
        yield mmsi

def split_ranges_by_week(ranges):
    """Break ranges up so each subrange falls in a single year.


    Parameters
    ==========          
    ranges: dict mapping mmsi to (start_time, stop_time) times

    Yields
    ======
    Ranges:
        Range start_time, stop_time are in the same year.

    """
    # Determine the year range we are concerned with
    start = None
    stop = None
    for (_, t0, t1, _) in ranges:
        if start is None:
            start = t0
        if stop is None:
            stop = t1
        start = min(start, t0)
        stop = max(stop, t1)
    # Yield ranges for each week of interest
    week_start = start.date()
    stop_date = stop.date()
    while week_start <= stop_date:
        week_stop = week_start + datetime.timedelta(days=6)
        yield (week_start, week_stop, _yield_ranges_within_dates(week_start, week_stop, ranges))
        week_start += datetime.timedelta(days=7)


N_SIMULTANEOUS_QUERIES = 10


def download_ais_and_join_ranges(ranges, gcs_temp_dir, local_temp_path):
    """Download corresponding AIS and join with supplied ranges.

    Given a set of ranges:
        * Download the corresponding AIS data from BigQuery
        * Join the fishing activity in the ranges to the AIS data
        * Yield lines that correspond to a CSV file
    

    Parameters
    ==========        
    ranges: seq of Ranges

    gcs_temp_dir: str
        gcs path to directory for storing temporary files
    local_temp_path: str
        local path for storing temporary files

    Yields
    ======
    lines of CSV file representing downloaded data.
    CAUTION: output is not gauranteed to be sorted by time.

    """
    # In order to avoid hitting BQ too hard we query across all of the MSI
    # at once, but limit the query length to one week
    grouped_by_mmsi = group_ranges_by_mmsi(ranges)
    consolidated_ranges = consolidate_ranges(grouped_by_mmsi)
    split_ranges = split_ranges_by_week(consolidated_ranges)
    # `mmsi_map` maps the GCS paths where BQ data is extracted to
    # onto the range they were extracted for. This is used later
    # process the range when the extraction is complete.
    mmsi_map = {}
    queries = []
    # Setup all of the queries to run in parallel
    i = 0
    for (t0, t1, mmsi_iter) in split_ranges:
        mmsi = list(mmsi_iter)
        if mmsi:
            gcs_path = gcs_temp_dir + "range_{}.csv".format(i)
            table = "scratch_{0}".format(i)
            temp_dest = {'dataset': 'scratch_fishing_score', 'table': table}
            query = create_query(",".join(mmsi), t0, t1)
            mmsi_map[gcs_path] = mmsi
            queries.append(
                dict(
                    timeout=120,
                    proj_id=proj_id,
                    query=query,
                    path=gcs_path,
                    temp_dest=temp_dest,
                    priority="BATCH",
                    compression="NONE"))
            i += 1
            #
    bigq = bqtools.BigQuery()
    header = None
    # This runs all of the BQ queries in parallel. As each
    # query finishes, this yields the location that query
    # was written to. The order of finishing is not
    # deterministic.
    for gcs_path in bigq.parallel_query_and_extract(queries):
        # As each query finishes join it with the ranges for the MMSI 
        # associated with that query..
        # We determine the MMSI by looking it up in `mmsi_map` that
        # was constructed when setting up the queries.
        mmsi = mmsi_map[gcs_path]
        bqtools.gs_mv(gcs_path, local_temp_path)
        # Read in the time points for each mmsi from AIS data (first pass)
        for m in mmsi:
            times = []
            with open(local_temp_path) as f:
                for row in csv.DictReader(f):
                    if row['mmsi'] == m:
                        times.append(dateutil.parser.parse(row['timestamp']))
            # Use the time points to generate a fishing series based on the
            # ranges for this MMSI. Because the data on disk is not sorted,
            # we use argsort so that we match things up on our second pass.
            indices = np.argsort(times)
            permuted_times = [times[i] for i in indices]
            # convert the times to fishing scores using ranges
            permuted_series = create_fishing_series(permuted_times, grouped_by_mmsi[m])
            series = [None] * len(permuted_series)
            for i, ndx in enumerate(indices):
                series[ndx] = permuted_series[i]
            assert all((x != None) for x in series)
            # scan the files again and now add fishing information
            with open(local_temp_path) as f:
                reader = csv.DictReader(f)
                this_header = ",".join(reader.fieldnames) + ",classification"
                if header is None:
                    header = this_header
                    yield header
                assert this_header == header
                fishing_iter = iter(series)
                for row in reader:
                    if row['mmsi'] == m:
                        is_fishing = next(fishing_iter)
                        if is_fishing != -1:
                            yield ",".join(row[x] for x in reader.fieldnames) +  "," + str(is_fishing)



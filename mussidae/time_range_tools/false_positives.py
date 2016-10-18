from __future__ import absolute_import
import os
import numpy as np
from numpy.lib.recfunctions import append_fields
import dateutil.parser
import logging
import csv
from cStringIO import StringIO
import mussidae.time_range_tools as trtools

import bqtools

proj_id = 'world-fishing-827'
base_path = 'gs://world-fishing-827/scratch/classification/range_queries/'
destination_dir = "temp_data"

NOT_FISHING = 0


def make_ranges(line_iter):
    header = next(line_iter)
    assert header.strip() == "reported by,MMSI,ts1,ts2,workspace", header
    for line in line_iter:
        try:
            _, mmsi, ts1, ts2, _ = (x.strip() for x in line.split(','))
            start = trtools.parse_timestamp(ts1)
            end = trtools.parse_timestamp(ts2)
            if end < start:  # It happens!
                start, end = end, start
        except StandardError as err:
            logging.warning("Could not interpret line, skipping")
            logging.warning(line.strip())
        else:
            yield mmsi, start, end, NOT_FISHING


fields = [
    "mmsi",
    "timestamp",
    # "seg_id",  # TODO[bitsofbits] look into what the best pipeline to pull this stuff from is.
    # "distance_from_shore", # TODO[bitsofbits]: these fields not present for all dates. investigate
    # "distance_from_port",
    "speed",
    "course",
    "lat",
    "lon"
]

query_template = """
SELECT
    {fields}
FROM
   TABLE_DATE_RANGE([pipeline_normalize.],
                    TIMESTAMP('{start.year}-{start.month}-{start.day}'),
                    TIMESTAMP('{stop.year}-{stop.month}-{stop.day}'))
WHERE
  (lat is not null) AND (lon is not null) AND
  (mmsi == {mmsi})

LIMIT  {limit};
"""


def create_query(mmsi, start, stop, limit=10000000):
    fields_str = ",\n".join(fields)
    return query_template.format(
        start=start, stop=stop, limit=limit, fields=fields_str, mmsi=mmsi)


def parse(x):
    dt = dateutil.parser.parse(x)
    return float(dt.strftime("%s"))




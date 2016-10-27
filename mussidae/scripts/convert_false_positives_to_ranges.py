"""

python scripts/convert_false_positives_to_ranges.py  \\
    --source-path data-precursors/time-range-sources/GFW\\ Fishing\\ Detection\\ False\\ Positives\\ -\\ false\\ positives.csv \
    --dest-path data/time-ranges/false_positives.csv
"""
from __future__ import absolute_import
import os
import numpy as np
from numpy.lib.recfunctions import append_fields
import dateutil.parser
import logging
import csv
from cStringIO import StringIO
import mussidae.time_range_tools as trtools
from mussidae.time_range_tools import false_positives


if __name__ == "__main__":
    import argparse
    import glob
    import os
    with open("data-precursors/time-range-sources/non-public-sources/SALT") as f:
        salt = f.read().strip()
    np.random.seed(hash(salt) % 4294967295)
    logging.getLogger().setLevel("WARNING")
    parser = argparse.ArgumentParser(
        description="Convert false positive csv file to time ranges")
    parser.add_argument(
        '--source-path',
        help='path to source false positive csv file',
        required=True)
    parser.add_argument(
        '--dest-path', help='path to dest message file', required=True)
    args = parser.parse_args()
    with open(args.source_path) as f:
        ranges_by_mmsi = false_positives.make_ranges(f)
        ranges = trtools.anonymize_ranges(ranges_by_mmsi, salt)
        trtools.write_ranges(ranges, args.dest_path)

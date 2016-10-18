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
        ranges = false_positives.make_ranges(f)
        trtools.write_ranges(ranges, args.dest_path)

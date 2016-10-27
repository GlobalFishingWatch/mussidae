"""

Example:

python scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../../vessel-scoring/datasets/kristina_longliner \
    --dest-path data/time-ranges/kristina_longliner.csv

 python scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../../vessel-scoring/datasets/kristina_trawl \
    --dest-path data/time-ranges/kristina_trawl.csv

 python scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../../vessel-scoring/datasets/kristina_ps \
    --dest-path data/time-ranges/kristina_ps.csv

"""
import argparse
import glob
import os
import mussidae.time_range_tools as trtools
import mussidae.time_range_tools.dialects as dialects
import numpy as np

if __name__ == "__main__":
    with open("data-precursors/time-range-sources/non-public-sources/SALT") as f:
        salt = f.read().strip()
    np.random.seed(hash(salt) % 4294967295)
    parser = argparse.ArgumentParser(
        description="extract fishing/nonfishing ranges from Kristina's data")
    parser.add_argument(
        '--source-dir', help='directory holding sources to convert')
    parser.add_argument('--dest-path', help='path to write results to')
    args = parser.parse_args()
    in_paths = glob.glob(os.path.join(args.source_dir, "*.csv"))
    ranges_by_mmsi = trtools.ranges_from_paths(in_paths, dialects.kristina)
    ranges = trtools.anonymize_ranges(ranges_by_mmsi, salt)
    trtools.write_ranges(sorted(ranges), args.dest_path)

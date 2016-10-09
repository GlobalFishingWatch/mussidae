"""

Example:

python mussidae/scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../vessel-scoring-time-ranges/datasets/kristina_longliner \
    --dest-path mussidae/data/time-ranges/kristina_longliner.csv

 python mussidae/scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../vessel-scoring-time-ranges/datasets/kristina_trawl \
    --dest-path mussidae/data/time-ranges/kristina_trawl.csv

 python mussidae/scripts/convert_kristinas_data_to_ranges.py \
    --source-dir ../vessel-scoring-time-ranges/datasets/kristina_ps \
    --dest-path mussidae/data/time-ranges/kristina_ps.csv

"""
import argparse
import glob
import os
import mussidae.time_range_tools as trtools
import mussidae.time_range_tools.dialects as dialects


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extract fishing/nonfishing ranges from Kristina's data")
    parser.add_argument('--source-dir', help='directory holding sources to convert')
    parser.add_argument('--dest-path', help='path to write results to')
    args = parser.parse_args()
    in_paths = glob.glob(os.path.join(args.source_dir, "*.csv"))
    results = trtools.ranges_from_paths(in_paths, dialects.kristina)
    with open(args.dest_path, "w") as f:
        f.write("mmsi,start_time,end_time,is_fishing\n")
        for row in results:
            f.write("{}\n".format(','.join(str(x) for x in row)))
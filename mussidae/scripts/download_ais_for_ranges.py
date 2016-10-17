"""Download AIS data from BigQuery and merge with ranges

NOTE: temp

"""
from mussidae import time_range_tools as trtools
from mussidae.time_range_tools.download_ais import download_ais_and_join_ranges
import itertools
import logging
import numpy as np
import os
from cStringIO import StringIO
import dateutil.parser
import tempfile

def parse(x):
    dt = dateutil.parser.parse(x)
    return float(dt.strftime("%s"))

gc_temp_dir = 'gs://world-fishing-827/scratch/mussidae/'

if __name__ == "__main__":
    import argparse
    import glob
    import os
    logging.getLogger().setLevel("WARNING")
    parser = argparse.ArgumentParser(description="Download AIS data from BigQuery and merge with ranges.\n"
                                                 "Note that temporary data are stored in `scipts/temp-data`.")
    parser.add_argument(
        '--source-paths', help='path to csv file holding ranges', nargs="+")
    parser.add_argument('--dest-path', help='path to output file (npz or csv)', required=True)
    args = parser.parse_args()
    #
    ranges = itertools.chain(*[trtools.load_ranges(p) for p in args.source_paths]) 
    local_temp_path = os.path.join(tempfile.gettempdir(), "download_ais.tmp")
    try:
        downloaded = download_ais_and_join_ranges(ranges, gc_temp_dir, local_temp_path)
        if args.dest_path.endswith('.csv'):
            with open(args.dest_path) as f:        
                for row in downloaded:
                    f.write(','.join(str(x) for x in row) + '\n')
        elif args.dest_path.endswith('.npz'):
            data = np.recfromcsv(
                StringIO('\n'.join(downloaded)),
                delimiter=',',
                filling_values=np.nan,
                converters={'timestamp': parse})
            np.savez(args.dest_path, x=data)
        else:
            raise ValueError("unknown suffix ({})".format(
                os.splitext(dest_path)[1]))
    finally:
        os.unlink(local_temp_path)

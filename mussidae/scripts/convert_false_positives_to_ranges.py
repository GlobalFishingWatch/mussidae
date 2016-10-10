import os
import numpy as np
from numpy.lib.recfunctions import append_fields
import dateutil.parser
import logging
import csv
from cStringIO import StringIO
import mussidae.time_range_tools as trtools
from  mussidae.time_range_tools import false_positives



def download_from_bq(ranges):
    range_map = {}
    queries = []
    for i, rng in enumerate(ranges):
        (mmsi, start, end, _) = rng
        gcs_path = base_path + "range_{}.csv".format(i)
        table = "scratch_{0}".format(i)
        temp_dest={'dataset': 'scratch_fishing_score', 'table': table}
        query = create_query(mmsi, start, end)
        range_map[gcs_path] = rng
        queries.append(dict(
            proj_id=proj_id,
            query=query,
            path=gcs_path,
            temp_dest=temp_dest,
            compression="NONE"))
        # if i > 2:
        #     break # XXX
    bigq = bqtools.BigQuery()
    rows = []
    header = None
    for gcs_path in bigq.parallel_query_and_extract(queries):
        rng = range_map[gcs_path]
        bqtools.gs_mv(gcs_path, destination_dir) 
        local_path = os.path.join(destination_dir, os.path.basename(gcs_path))
        tail = ",{}".format(rng[-1])
        with open(local_path) as f:
            f_iter = iter(f)
            this_header = next(f_iter).strip() + ",classification"
            if header is None:
                header = this_header
                rows.insert(0, header)
            assert this_header == header
            for row in f_iter:
                row = row.strip()
                if row:
                    rows.append(row + tail)
    f = StringIO('\n'.join(rows))
    data = np.recfromcsv(f, delimiter=',', filling_values=np.nan, converters={'timestamp' : parse})
    del f
    return data



if __name__ == "__main__":
    import argparse
    import glob
    import os
    logging.getLogger().setLevel("WARNING")
    parser = argparse.ArgumentParser(description="Convert false positive csv file to time ranges")
    parser.add_argument('--source-path', help='path to source false positive csv file')
    parser.add_argument('--dest-path', help='path to dest message file')
    args = parser.parse_args()
    with open (args.source_path) as src:
        ranges = false_positives.make_ranges(f)
        trtools.write_ranges(ranges, args.dst_path)


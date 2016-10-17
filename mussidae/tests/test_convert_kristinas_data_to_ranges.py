from __future__ import print_function
from __future__ import division
import numpy as np
import pandas as pd
import datetime
import logging
import mussidae.time_range_tools as trtools
import mussidae.time_range_tools.dialects as dialects
import mussidae.time_range_tools.create_fishing_series as create_fseries
import tempfile


def count_problem_duplicates(times, fishing):
    assert create_fseries.is_sorted(times)
    last_timestamp = None
    last_fishing = None
    problem_count = 0
    series_count = 1
    problematic = False
    for timestamp, is_fishing in zip(times, fishing):
        is_fishing = None if is_fishing in (-1, 2) else is_fishing
        if timestamp == last_timestamp:
            if problematic:
                problem_count += 1
            else:
                series_count += 1
                if is_fishing != last_fishing:
                    problem_count += series_count
                    problematic = True
        else:
            problematic = False
            series_count = 1
            last_timestamp = timestamp
            last_fishing = is_fishing
    return problem_count


def test_round_trip(source_paths, ranges):
    for pth in in_paths:
        logging.info("testing file: {}".format(pth))
        all_examples = pd.read_csv(pth)
        mmsi = sorted(set(all_examples['MMSI']))
        for m in mmsi:
            logging.info("testing mmsi: {}".format(m))
            examples = all_examples[all_examples['MMSI'] == m]
            times = np.array([dialects.get_kristina_timestamp(x)
                              for (_, x) in examples.iterrows()])
            fishing = np.array(
                [dialects.get_kristina_is_fishing(x)
                 for (_, x) in examples.iterrows()],
                dtype=bool)
            ndx_map = np.argsort(times)
            ranges_for_mmsi = (x for x in ranges if x.mmsi == m)
            permuted_results = create_fseries.create_fishing_series(times[ndx_map],
                                                          ranges_for_mmsi)
            #
            results = np.zeros_like(permuted_results)
            results[ndx_map] = permuted_results
            #
            unknown_mask = (results == -1)
            correct = np.alltrue((results == fishing) | unknown_mask)
            if not correct:
                logging.error("{}: {} failed".format(pth, m))
            n_unknowns = unknown_mask.sum()
            n_problems_dups = count_problem_duplicates(times[ndx_map],
                                                       fishing[ndx_map])
            if n_unknowns != n_problems_dups:
                logging.warning("{}%({} - {}) of samples unknown for {}: {}".
                                format(100 * (n_unknowns - n_problems_dups
                                              ) / len(unknown_mask),
                                       n_unknowns, n_problems_dups, pth, m))




if __name__ == "__main__":
    import argparse
    import glob
    import os
    logging.getLogger().setLevel("WARNING")
    parser = argparse.ArgumentParser(
        description="extract fishing/nonfishing ranges from Kristina's data")
    parser.add_argument(
        '--source-dir',
        help='directory where converted sources were drawn from',
        default="../../vessel-scoring/datasets/kristina_longliner")
    args = parser.parse_args()
    in_paths = glob.glob(os.path.join(args.source_dir, "*.csv"))
    ranges = list(trtools.ranges_from_paths(in_paths, dialect=dialects.kristina))
    test_round_trip(in_paths, ranges)


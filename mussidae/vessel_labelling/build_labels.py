from __future__ import print_function, division
import argparse
import logging
import datetime
import os

from vessel_label_mapping import build_labels

if __name__ == '__main__':
    today = datetime.date.today()
    default_path = 'data/classification-list-{:04}-{:02}-{:02}.csv'.format(
        today.year, today.month, today.day)
    parser = argparse.ArgumentParser(
        description='Build vessel label mapping from multiple source files.')
    parser.add_argument(
        '--output_csv', help='Output file name.', default=default_path)
    parser.add_argument(
        '--source_csv_dir',
        help='Path to directory containing input vessel lists.',
        default='data-precursors/classification-list-sources')
    parser.add_argument(
        '--log',
        help='Set the logging level.',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default="WARNING")
    args = parser.parse_args()
    log_level = getattr(logging, args.log.upper(), None)
    logging.basicConfig(level=log_level)
    build_labels(logging, args.source_csv_dir, args.output_csv)

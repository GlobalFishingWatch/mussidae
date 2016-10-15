"""

The fields of a Dialect object are getters to be used on a loaded csv file to grab
the items in the appropriate format. Current supported are:

    * mmsi
    * timestamp
    * is_fishing

"""

from __future__ import print_function
from __future__ import division
import datetime
import pytz
from collections import namedtuple
from .utils import parse_timestamp

Dialect = namedtuple("Dialect", "mmsi timestamp is_fishing")


def get_kristina_timestamp(x):
    if 'DATETIME' in x:
        return parse_timestamp(x['DATETIME'])
    elif 'TIME' in x:
        return datetime.datetime.strptime(x['TIME'], "%Y%m%d_%H%M%S").replace(
            tzinfo=pytz.utc)
    else:
        assert False, "NO TIME: {}".format(x)


def get_kristina_is_fishing(x):
    if 'COARSE_FIS' in x:
        return float(x['COARSE_FIS'])
    else:
        return float(x['COARSE-FIS'])


kristina = Dialect(lambda x: int(x['MMSI']), get_kristina_timestamp,
                   get_kristina_is_fishing)

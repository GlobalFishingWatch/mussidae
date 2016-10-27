import numpy as np
import sys
import hashlib
from .utils import Range


def mmsi_to_id(mmsi, salt, hexdigits=12):
    h = hashlib.sha1()
    h.update(str(salt))
    h.update(str(mmsi))
    return int(h.hexdigest()[:hexdigits], 16)


def anonymize_ranges(ranges, salt):
    for mmsi, start, stop, is_fishing in ranges:
        yield(mmsi_to_id(mmsi, salt), start, stop, is_fishing)



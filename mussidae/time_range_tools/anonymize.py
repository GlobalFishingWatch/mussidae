import numpy as np
import sys
import hashlib


def mmsi_to_id(mmsi, salt, hexdigits=12):
    h = hashlib.sha1()
    h.update(str(salt))
    h.update(str(mmsi))
    return int(h.hexdigest()[:hexdigits], 16)



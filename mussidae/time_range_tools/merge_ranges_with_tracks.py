from __future__ import print_function
from __future__ import division
import numpy as np
import dateutil


def is_sorted(x):
	last = x[0]
	for this in x[1:]:
		if this < last:
			return False
		last = this
	return True



def create_fishing_series(mmsi, times, ranges):
	"""

	Parameters
	==========
	mmsi : str

	times : sequence of datetime
		Sequence must be sorted

	ranges: sequence of (mmsi, start_time, end_time, is_fishing)
		mmsi : str
		start_time : str in ISO 8601 format
		stop_time : str in ISO 8601 format
		is_fishing : boolean

	Returns
	=======
	sequence of {0, 1, -1}
		whether the vessel is fishing at given point or -1 for
		don't know

	"""
	if not is_sorted(times):
		raise ValueError("times must be sorted")
	# Only look at ranges associated with the current mmsi
	ranges = ranges[ranges['mmsi'] == mmsi]
	# Initialize is_fishing to -1 (don't know)
	is_fishing = np.empty([len(times)], dtype=int) 
	is_fishing.fill(-1)
	#
	for _, (_, startstr, endstr, state) in ranges.iterrows():
		start = dateutil.parser.parse(startstr)
		end = dateutil.parser.parse(endstr)
		i0 = np.searchsorted(times, start, side="left")
		i1 = np.searchsorted(times, end, side="right")
		is_fishing[i0: i1] = state
	#
	return is_fishing





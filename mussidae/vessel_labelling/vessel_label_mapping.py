# Aggregate vessel labels from multiple csv files into one master file.
import collections
import csv
import hashlib
import math
import os
import struct
import numpy as np


def _utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


EXTRA_SALT = "extra_salt"

# The minimum number of messages for a vessel track to be usable.
_MIN_MESSAGES_FOR_USABLE_TRACK = 1000

_CARGO_TANKER = 'Cargo/Tanker'
_LONGLINER = 'Longliner'
_PASSENGER = 'Passenger'
_POTS_AND_TRAPS = 'Pots and traps'
_PURSE_SEINE = 'Purse seine'
_SEISMIC = 'Seismic'
_SQUID_FISHING = 'Squid fishing'
_TRAWLER = 'Trawler'
_TUG_PILOT_SUPPLY = 'Tug/Pilot/Supply'

_DEFAULT_MAPPING = {
    'Tug': _TUG_PILOT_SUPPLY,
    'Passenger': _PASSENGER,
    'Cargo': _CARGO_TANKER,
    'Tanker': _CARGO_TANKER,
    'Purse seine': _PURSE_SEINE,
    'seismic vessel': _SEISMIC,
    'Squid fishing': _SQUID_FISHING,
    'Trawler': _TRAWLER,
    'Longliner': _LONGLINER,
}

# Fish carriers and pole and line have insufficient vessels to train a class.
_CLAV_MAPPING = {
    'Purse seiners': _PURSE_SEINE,
    'Tuna purse seiners': _PURSE_SEINE,
    'Trawlers': _TRAWLER,
    'Longliners': _LONGLINER,
    'Tuna longliners': _LONGLINER,
}

_ITU_MAPPING = {
    'FBT': _PASSENGER,
    'PA': _PASSENGER,
    'TUG': _TUG_PILOT_SUPPLY,
    'LOU': _PASSENGER,
    'GOU': _PASSENGER,
    'SLO': _PASSENGER,
    'VLR': _PASSENGER,
    'YAT': _PASSENGER,
    'RAV': _TUG_PILOT_SUPPLY,
    'LAN': _POTS_AND_TRAPS,
}

_EU_VESSEL_MAPPING = {
    'FPO': _POTS_AND_TRAPS,
    'OTB': _TRAWLER,
    'OTM': _TRAWLER,
    'OTT': _TRAWLER,
    'PS': _PURSE_SEINE,
    'PTB': _TRAWLER,
    'PTM': _TRAWLER,
    'TBB': _TRAWLER,
}
"""The proportion of vessels to assign to training (rather than test) set."""
_TRAINING_SET_PROPORTION = 0.6


def _hash_mmsi_to_double(mmsi, salt):
    """Take a value and hash it to return a value in the range [0, 1.0).

   To be used as a deterministic probability for vessel dataset
   assignment: e.g. if we decide vessels should go in the training set at
   probability 0.2, then we map from mmsi to a probability, then if the value
   is <= 0.2 we assign this vessel to the training set.

  Args:
    mmsi: the input MMSI as an integer.
    salt: a salt concatenated to the mmsi to allow more than one value to be
          generated per mmsi.

  Returns:
    A value in the range [0, 1.0).
  """
    hasher = hashlib.md5()
    i = '%s_%s' % (mmsi, salt)
    hasher.update(i)

    # Pick a number of bytes from the bottom of the hash, and scale the value
    # by the max value that an unsigned integer of that size can have, to get a
    # value in the range [0, 1.0)
    hash_bytes_for_value = 4
    hash_value = struct.unpack('I', hasher.digest()[:hash_bytes_for_value])[0]
    sample = float(hash_value) / math.pow(2.0, hash_bytes_for_value * 8)
    assert sample >= 0.0
    assert sample <= 1.0
    return sample


class Dataset(object):
    """Represents one possible mapping from MMSI to vessel type.

  Attributes:
    _filename: the name of the CSV file containing this mapping.
    _mmsi_column: the column containing vessel MMSIs.
    _label_column: the column containing the vessel type.
    _mapping: a dictionary mapping from the vessel types in this file to our
              canonical labels.
  """

    def __init__(self, filename, mmsi_column, label_column, mapping):
        self._filename = filename
        self._mmsi_column = mmsi_column
        self._label_column = label_column
        self._mapping = mapping

    def parse(self, logging, vessel_map):
        """Reads and translates the vessel type mapping.

     For the given file, read and translate the vessel type mapping and
     populate the provided vesselmap dictionary.

    Args:
      logging: Logging module to report against.
      vessel_map: A dictionary from mmsi to (dataset, vessel label) updated with
                  the mappings in the current file.
    """
        with open(self._filename, 'r') as csvfile:
            reader = csv.DictReader(
                [row for row in csvfile if len(row) and row[0] != '#'])
            missing_labels = collections.Counter()
            for row in reader:
                mmsi = int(row[self._mmsi_column])
                label = row[self._label_column]
                if label in self._mapping:
                    # Get a random value in the range [0 - 1.0] from hashing the mmsi and
                    # use it to assign this vessel to a dataset.
                    p = _hash_mmsi_to_double(mmsi, '')
                    dataset = 'Training' if p < _TRAINING_SET_PROPORTION else 'Test'
                    vessel_map[mmsi] = (dataset, self._mapping[label])
                else:
                    missing_labels[label] += 1

        logging.info('For filename %s, missing labels: %s', self._filename,
                     missing_labels)


def get_datasets(destination_path):
    """Create a dictionary of datasets to processes.

  Vessel lists are in ascending priority order. Later lists override earlier if
  mmsis are duplicated.

  Args:
    destination_path: location of the CSV files to load.

  Returns:
    A list of datasets.
  """

    return [
        Dataset(
            os.path.join(destination_path, 'ITU_Dec_2015_full_list.csv'),
            mmsi_column='MMSI',
            label_column='Individual classification',
            mapping=_ITU_MAPPING),
        Dataset(
            os.path.join(destination_path, 'CLAVRegistryMatchingv5.csv'),
            mmsi_column='mmsi',
            label_column='shiptype',
            mapping=_CLAV_MAPPING),
        Dataset(
            os.path.join(destination_path, 'KnownVesselCargoTanker.csv'),
            mmsi_column='mmsi',
            label_column='label',
            mapping=_DEFAULT_MAPPING),
        Dataset(
            os.path.join(destination_path, 'KristinaManualClassification.csv'),
            mmsi_column='mmsi',
            label_column='label',
            mapping=_DEFAULT_MAPPING),
        Dataset(
            os.path.join(destination_path, 'PyBossaNonFishing.csv'),
            mmsi_column='mmsi',
            label_column='label',
            mapping=_DEFAULT_MAPPING),
        Dataset(
            os.path.join(destination_path, 'AlexWManualNonFishing.csv'),
            mmsi_column='mmsi',
            label_column='label',
            mapping=_DEFAULT_MAPPING),
        Dataset(
            os.path.join(destination_path, 'EUFishingVesselRegister.csv'),
            mmsi_column='mmsi',
            label_column='Gear_Main_Code',
            mapping=_EU_VESSEL_MAPPING),
        Dataset(
            os.path.join(destination_path, 'PeruvianSquidFleet.csv'),
            mmsi_column='mmsi',
            label_column='label',
            mapping=_DEFAULT_MAPPING),
        Dataset(
            os.path.join(destination_path,
                         'WorldwideSeismicVesselDatabase4Dec15.csv'),
            mmsi_column='MMSI #',
            label_column='Label',
            mapping=_DEFAULT_MAPPING),
    ]


def get_message_counts(mmsi_count_path):
    counts = {}
    with open(mmsi_count_path, 'r') as csvfile:
        reader = csv.DictReader([row for row in csvfile
                                 if len(row) and row[0] != '#'])
        for row in reader:
            counts[int(row['mmsi'])] = int(row['count'])

    return counts


def build_labels(logging, source_path, output_filename):
    """Consolidate vessel labels from multiple sources and write to one csv.

   For the given source path, read a predefined set of prioritised vessel
   mappings and consolidate and write to a single file.

  Args:
    logging: Logging module to report against.
    source_path: Input path to read source label csvs.
    output_filename: Filename to write consolidated labels.
  """
    # Bring in a snapshot of the number of messages per vessel (keyed by mmsi) so
    # that when we make a choice of vessels from the lists, we do not include ones
    # that have no or insufficient data. Note that this source file is generated
    # by a Dremel query and should be updated every now and again.
    message_counts = get_message_counts(
        os.path.join(source_path, 'MssiMessageCounts.csv'))

    mapping = {}
    for ds in get_datasets(source_path):
        ds.parse(logging, mapping)

    vessel_list = []
    dataset_vessel_count_map = collections.Counter()
    label_vessel_count_map = collections.Counter()
    for mmsi, (dataset, labels) in mapping.items():
        if mmsi in message_counts and message_counts[
                mmsi] >= _MIN_MESSAGES_FOR_USABLE_TRACK:
            vessel_list.append((mmsi, dataset, labels))
            dataset_vessel_count_map[dataset] += 1
            label_vessel_count_map[labels] += 1

    logging.info('Dataset label count: %s', str(dataset_vessel_count_map))
    logging.info('Class label count: %s', str(label_vessel_count_map))

    vessel_list.sort()

    with open(output_filename, 'w') as output_file:
        output_file.write("mmsi,dataset,label\n")
        for i, (mmsi, dataset, label) in enumerate(vessel_list):
            output_file.write('%d,%s,%s,%s\n' %
                              (mmsi, dataset, label))

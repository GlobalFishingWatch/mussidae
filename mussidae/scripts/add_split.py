from __future__ import print_function, division
from glob import glob
from collections import defaultdict
from collections import Counter
from collections import namedtuple
from sklearn.model_selection import StratifiedKFold
import json
import csv
import logging
import os
import numpy as np
import yaml
logging.getLogger().setLevel('INFO')


schema = yaml.load('''
non_fishing:
  passenger:
  gear:
  fish_factory:
  cargo_or_tanker:
    bunker_or_tanker:
      bunker:
      tanker:
    cargo_or_reefer:
      cargo:
      reefer:
        specialized_reefer:
        container_reefer:
      fish_tender:
        well_boat:
  patrol_vessel:
  research:
  dive_vessel:
  submarine:
  dredge_non_fishing:
  supply_vessel:
  tug:
  seismic_vessel:
  helicopter:
  other_not_fishing:

fishing:
  squid_jigger:
  drifting_longlines:
  pole_and_line:
  other_fishing:
  trollers:
  fixed_gear:
    pots_and_traps:
    set_longlines:
    set_gillnets:
  trawlers:
  dredge_fishing:
  seiners:
   purse_seines:
    tuna_purse_seines:
    other_purse_seines:
   other_seines:
  driftnets:
''')


def atomic(obj):
    for k, v in obj.items():
        if v is None:
            yield k
        else:
            for x in atomic(v):
                yield x


simple_labels = set(atomic(schema))


def categories(obj, include_atomic=True):
    for k, v in obj.items():
        if v is None:
            if include_atomic:
                yield k, [k]
        else:
            yield (k, list(atomic(v)))
            for x in categories(v, include_atomic=include_atomic):
                yield x

all_labels = dict(categories(schema))


def load_list(path):
    name = os.path.splitext(os.path.basename(path))[0]
    logging.info('Processing: %s', name)
    data = []
    with open(path, 'rU') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        return list(reader), fieldnames


# 
# Assign to Test / Training splits
#

# Don't assign any class with fewer than MIN_COUNT examples to the test split.
MIN_COUNT = 20


def assign_splits(rows, seed=4321):
    """



    """
    # Determine eligible test labels: 
    #   Only simple labels (no '|') that aren't in excluded are eligible
    #   and they must have at least MIN_COUNT examples
    #   Not in excluded

    possible_test_labels = simple_labels | {'unknown'}

    labels = [x.get('geartype', 'unknown') for x in rows]

    counts = Counter(labels)
    test_labels = {x for x in labels if x in possible_test_labels and counts[x] > MIN_COUNT} 

    cand_indices = [i for (i, l) in enumerate(labels) if l in test_labels]
    cand_labels = [l for (i, l) in enumerate(labels) if l in test_labels]
    #
    folder = StratifiedKFold(n_splits=2, shuffle=True, random_state=seed)
    #
    test_indices_indices = list(folder.split(cand_indices, cand_labels))[0][0]
    test_indices = set([cand_indices[x] for x in test_indices_indices])
    #
    for i, row in enumerate(rows):
        if not (row['geartype'] or row['tonnage'] or row['length'] 
                or row['engine_power'] or row['crew']):
            row['split'] = None # Skip if no information
        else:
            row['split'] = 'Test' if (i in test_indices) else 'Training'



def dump(path, lines, fieldnames):
    fieldnames.extend(['split', 'label', 'crew_size'])
    mmsi_seen = set()
    with open(path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        for row in lines:
            if not row['mmsi']:
                continue
            mmsi = int(float(row['mmsi']))

            row['label'] = row['geartype']
            if row['label'] == '':
                row['label'] = 'unknown'
            else:
                for x in row['label'].split('|'):
                    if x not in all_labels:
                        print("XXX", mmsi, row['geartype'])
            row['crew_size'] = row['crew']
            if mmsi in mmsi_seen:
                # print('duplicate mmsi', mmsi, 'skipping')
                continue
            mmsi_seen.add(mmsi)
            row['mmsi'] = mmsi # TODO: remove once mmsi fixed in csv

            if row['split']:
                writer.writerow(row)
    print(len(lines) - len(mmsi_seen), "duplicates")


if __name__ == '__main__':
    this_directory = os.path.abspath(os.path.dirname(__file__))
    vessel_data, fieldnames = load_list(os.path.join(this_directory, "../data/unsplit_classification_list.csv"))
    assign_splits(vessel_data)
    dump(os.path.join(this_directory, "../data/classification_list.csv"), vessel_data, fieldnames)

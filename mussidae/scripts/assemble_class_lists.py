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
logging.getLogger().setLevel('INFO')


#
# Data used in loading lists
#

simple_labels = { 'cargo',
                 'drifting_longlines',
                 'motor_passenger',
                 'other_fishing',
                 'other_not_fishing',
                 'pole_and_line',
                 'pots_and_traps',
                 'purse_seines',
                 'reefer',
                 'sailing',
                 'seismic_vessel',
                 'set_gillnets',
                 'set_longlines',
                 'squid_jigger',
                 'tanker',
                 'trawlers',
                 'trollers',
                 'tug'}  

composite_labels = {'passenger',
                    'unknown_fishing',
                    'unknown_longline',
                    'unknown_not_fishing'}  

valid_labels = simple_labels | composite_labels

null_labels = set(['unknown', 'none', 'no_idea_what_it_is', '', None])

keys = ['mmsi', 'label', 'length', 'engine_power', 'tonnage']
assert keys[0] == 'mmsi', '"mmsi" must appear first in `keys`'

output_keys = keys + ['split', 'source']

# TODO: use class instead of namedtuple
VesselRecord = namedtuple("VesselRecord", output_keys)


#
# Data used in combining lists
#

fishing_classes = {  'drifting_longlines',
                     'other_fishing',
                     'pole_and_line',
                     'pots_and_traps',
                     'purse_seines',
                     'set_gillnets',
                     'set_longlines',
                     'squid_jigger',
                     'trawlers',
                     'trollers',
                     'unknown_longline'}

non_fishing_classes = {  'cargo',
                         'motor_passenger',
                         'other_not_fishing',
                         'passenger',
                         'reefer',
                         'sailing',
                         'seismic_vessel',
                         'tanker',
                         'tug'}

missing_classes = valid_labels - (fishing_classes | non_fishing_classes)

if missing_classes:
    print('Classes missing from fishing/nonfishing:', sorted(missing_classes))


#
# Functions for converting raw lists to a common format
#

class LabelConverter(object):
    """Use `mapping` to convert labels to canonical form

    Args:
        mapping : dict
            A map between labels used in a particular file and canonical labels
    """
    
    def __init__(self, mapping):
        if mapping is None:
            self.mapping = None
        else:
            self.mapping = {k.lower(): None if (v is None) else v.lower() for (k, v) in mapping.items()}
        
    def __call__(self, x, key):
        """Convert a label

        Args:
            x : str
                label to convert
            key : str
                used for error reporting

        Label is split on '|' to it constituent sublabels, then each
        label is converted using mapping and checked against the null
        labels possibilities.

        """
        # TODO: Clean up this logic a bit. Really only none should need to be
        # shortcircuited here.
        if x in null_labels:
            return ''
        if self.mapping and self.mapping.get(x, "NOT_IN_NULL") in null_labels:
            return ''
        result = []
        for sub_result in x.split('|'):
            sub_result = sub_result.strip().lower()
            if self.mapping:
                sub_result = self.mapping.get(sub_result, sub_result)
            if sub_result in null_labels:
                continue
            for sub_sub_result in sub_result.split('|'):
                if sub_sub_result in null_labels:
                    continue
                if sub_sub_result not in valid_labels:
                    logging.warning('Ignoring %s: %s (%s)', key, repr(x), sub_sub_result)
                    return ''
                result.append(sub_sub_result)
        if result:
            return '|'.join(result)
        else:
            return ''

    
def to_float(x, key):
    """Convert strings found in lists to floating point values

    Args:
        x : str
            string to convert
        key: str
            Which key this value corresponds to; used for error reporting

    * Commas are replaced with periods to support British numbers.

    * 'ft' are converted to meters

    """
    x = x.strip()
    if not x or x in ('NA', 'n/a'):
        return None
    if '.'  in x:
        # There are '.'s, so commas are placeholders
        x = x.replace(',', '')    
    if x.endswith('ft'):
        scale = 0.3048
        x = x[:-2].strip()
    else:
        scale = 1    
    try:
        return scale * float(x)
    except:
        logging.warn('Could not convert %s value %s to float', key, x)
        return None
    
    
def load_lists(directory):
    """Load and normalize lists

    Args:
        directory : str
            directory containing lists ('.csv') and metadata ('.json') files

    Lists are loaded from the directory and labels are normalized using 
    the matching metadata file. Scalar values are  converted to float 
    using the `to_float` function defined above.

    """
    mapping = defaultdict(lambda : [[] for x in output_keys])
    csv_paths = sorted(glob(os.path.join(directory, '*.csv')))
    for csv_pth in csv_paths:
        name = os.path.splitext(os.path.basename(csv_pth))[0]
        logging.info('Processing: %s', name)
        json_pth = os.path.splitext(csv_pth)[0] + '.json'
        with open(json_pth) as f:
            info = json.load(f)
        # Create converters
        map = info.get('mappings', {})
        converters = {}
        converters['label'] = LabelConverter(map)
        for lbl in keys[1:]:
            if lbl != 'label':
                converters[lbl] = to_float
        #
        headers = info['headers']
        mmsi_key = info['headers']['mmsi']
        try:
            with open(csv_pth, 'rU') as f:
                for line in csv.DictReader(f):
                    chunks = []
                    for i, key in enumerate(keys):
                        # TODO: make this less hacky
                        hkey = 'engine power' if (key == 'engine_power') else key
                        hdr = headers[hkey]
                        if hdr is None:
                            value = None
                        else:
                            try:
                                value = line[hdr]
                            except KeyError:
                                logging.fatal('could not find key ({}) in {}'.format(hdr, line.keys()))
                                raise
                            if key in converters:
                                value = converters[key](value, key)
                        chunks.append(value)
                    for i in range(len(keys)):
                        mapping[chunks[0]][i].append(chunks[i])
                    mapping[chunks[0]][-2].append(None)
                    mapping[chunks[0]][-1].append(name)
        except:
            logging.warning("Failed loading from: %s", csv_pth)
            raise
    for k in mapping:
        mapping[k] = VesselRecord(*(mapping[k]))
    return mapping
    
        
#
# Functions for combining fields when the same vessel is in multiple lists
#

removable = [
    ('unknown_fishing', fishing_classes),
    ('unknown_not_fishing', non_fishing_classes),
    ('unknown_longline', {'drifting_longlines', 'set_longlines'}),
]


def combine_classes(classes):
    """Combine multiple classes

    Args:
        classes : list of str

    Classes are first split at '|', then recombined into one larger
    joint class using '|'.  `Unknown_` classes are removed if a more
    specific class is preset as specified in `removable` above.

    """
    classes = [x for x in classes if x]
    if not classes:
        return None
    else:
        class_set = set()
        for class_group in classes:
            for cls in class_group.split('|'):
                class_set.add(cls)
        for cls, required in removable:
            if (cls in class_set) and (class_set & required):
                class_set.remove(cls)       
        return '|'.join(sorted(class_set))
    
    

def combine_scalars(values, alpha=0.1):
    """
    Combine scalar values by taking the mean of all set values.
    
    If the standard deviation is greater than alpha * mean, return None instead,
    since the values are not clustered. By default alpha is 0.1 (10%)
    
    """
    values = [x for x in values if x]
    if not values:
        return None
    try:
        mean = np.mean(values)
        stddev = np.std(values)
    except:
        print(values)
        raise
    if stddev > alpha * mean:
        return None
    return mean


def combine_names(names):
    return(';'.join(sorted(set(names))))


def combine_mmsi(values):
    """Check that all mmsi are equal"""
    mmsi = values[0]
    for x in values[1:]:
        assert x == mmsi
    return mmsi


def combine_fields(mapping):
    new_mapping = {}
    for mmsi, values in mapping.items():
        new_values = []
        for key, keyvalues in zip(output_keys, values):
            if key == 'label':
                new_values.append(combine_classes(keyvalues))
            elif key == 'mmsi':
                new_values.append(combine_mmsi(keyvalues))
            elif key == 'split':
                new_values.append(keyvalues)
            elif key == 'source':
                new_values.append(combine_names(keyvalues))
            else:
                new_values.append(combine_scalars(keyvalues))
        new_mapping[mmsi] = VesselRecord(*new_values)
    return new_mapping
            

def apply_corrections(combined, base_path):
    # Remove incorrect MMSI
    with open(os.path.join(base_path, 'incorrect_mmsi.csv')) as f:
        removed = []
        for line in csv.DictReader(f):
            mmsi = line['mmsi'].strip()
            if mmsi in combined:
                removed.append(mmsi)
                combined.pop(mmsi)
        logging.info('Removing incorrect MMSI: %s', ", ".join(removed))


    # Fix lengths
    with open(os.path.join(base_path, 'corrected_lengths.csv')) as f:
        for line in csv.DictReader(f):
            mmsi = line['mmsi'].strip()
            if mmsi in combined:
                length = float(line['length'])
                logging.info('Correcting length for MMSI: %s  (%s -> %s)', mmsi, combined[mmsi].length, length)
                l = list(combined[mmsi])
                l[keys.index('length')] = length
                combined[mmsi] = VesselRecord(*l)
                assert combined[mmsi].length == length

    # Fix tonnages
    with open(os.path.join(base_path, 'corrected_tonnages.csv')) as f:
        for line in csv.DictReader(f):
            mmsi = line['mmsi'].strip()
            if mmsi in combined:
                tonnage = float(line['tonnage'])
                logging.info('Correcting tonnage for MMSI: %s  (%s -> %s)', mmsi, combined[mmsi].tonnage, tonnage)
                l = list(combined[mmsi])
                l[keys.index('tonnage')] = tonnage
                combined[mmsi] = VesselRecord(*l)
                assert combined[mmsi].tonnage == tonnage


    # Fix powers
    with open(os.path.join(base_path, 'corrected_engine_powers.csv')) as f:
        for line in csv.DictReader(f):
            mmsi = line['mmsi'].strip()
            if mmsi in combined:
                power = float(line['engine_power']) if line['engine_power'] else None
                logging.info('Correcting engine power for MMSI: %s  (%s -> %s)', mmsi, combined[mmsi].engine_power, power)
                l = list(combined[mmsi])
                l[keys.index('engine_power')] = power
                combined[mmsi] = VesselRecord(*l)
                assert combined[mmsi].engine_power == power, (combined[mmsi].engine_power, power)


# 
# Assign to Test / Training splits
#

# TODO, have option to only update splits.

# Don't assign any class with fewer than MIN_COUNT examples to the test split.
MIN_COUNT = 20


def assign_splits(combined, seed=4321):
    """



    """
    # Determine eligible test labels: 
    #   Only simple labels (no '|') that aren't in excluded are eligible
    #   and they must have at least MIN_COUNT examples
    #   Not in excluded

    counts = Counter(x.label for x in combined.values())
    test_labels = {x.label for x in combined.values() 
                     if x.label in simple_labels and counts[x.label] > MIN_COUNT} 
    np.random.seed(seed)
    all_mmsi = combined.keys()
    np.random.shuffle(all_mmsi)
    cand_mmsi = [x for x in all_mmsi if combined[x].label in test_labels]
    cand_labels = [combined[x].label for x in all_mmsi if combined[x].label in test_labels]
    #
    folder = StratifiedKFold(n_splits=2, random_state=seed)
    #
    test_indices = list(folder.split(cand_mmsi, cand_labels))[0][0]
    test_mmsi = set([cand_mmsi[x] for x in test_indices])
    #
    for mmsi in combined:
        if combined[mmsi].label is None:
            split = None
        else:
            split = 'Test' if (mmsi in test_mmsi) else 'Training'
        lst = list(combined[mmsi])
        lst[-2] = split
        combined[mmsi] = VesselRecord(*lst)


def dump(combined, path):
    with open(path, 'w') as f:
        writer = csv.DictWriter(f, output_keys)
        writer.writeheader()
        for mmsi in sorted(combined):
            values = combined[mmsi]
            if any(values[1:-1]):
                d = {k : v for (k, v) in zip(output_keys, values)}
                writer.writerow(d)



if __name__ == '__main__':
    this_directory = os.path.abspath(os.path.dirname(__file__))
    raw_lists = load_lists(os.path.join(this_directory, "../data-precursors/classification-list-sources"))
    combined_lists = combine_fields(raw_lists)
    apply_corrections(combined_lists, os.path.join(this_directory, "../data-precursors"))
    assign_splits(combined_lists)
    dump(combined_lists, os.path.join(this_directory, "../data/classification_list.csv"))

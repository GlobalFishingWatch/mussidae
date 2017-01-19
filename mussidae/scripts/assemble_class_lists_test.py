from __future__ import print_function, division
from glob import glob
import numpy as np
import json
import unittest
from StringIO import StringIO
import assemble_class_lists
from assemble_class_lists import VesselRecord
import logging

logging.getLogger().setLevel('CRITICAL')

example_info = json.load(
    StringIO('''{"headers": {"engine power": null, "mmsi": "mmsi", "tonnage": "tonnage", 
        "length": "length", "label": "shiptype"}, 
        "mappings": {"Recreational_fishing" : "unknown_fishing", "Bunker": "Tanker", 
        "Research": "unknown", "Handliners": "other_fishing"}}'''))


class CheckConverters(unittest.TestCase):

    def test_LabelConverter(self):
        converter = assemble_class_lists.LabelConverter(example_info['mappings'])
        self.assertEqual(converter('Bunker', 'key1'), 'tanker')
        self.assertEqual(converter('Recreational_fishing', 'key2'), 'unknown_fishing')
        self.assertEqual(converter('Foo', 'key3'), '')


    def test_to_float(self):
        self.assertEqual(assemble_class_lists.to_float('0.3', 'key1'), 0.3)
        self.assertEqual(assemble_class_lists.to_float('1 ft', 'key2'), 0.3048)
        self.assertEqual(assemble_class_lists.to_float('1,3', 'key3'), 1.3)
        self.assertEqual(assemble_class_lists.to_float('malformed', 'key4'), None)


    """Convert strings found in lists to floating point values

    Args:
        x : str
            string to convert
        key: str
            Which key this value corresponds to; used for error reporting

    * Commas are replaced with periods to support British numbers.

    * 'ft' are converted to meters

    """


class CheckCombines(unittest.TestCase):

    def test_combine_classes(self):
        self.assertEqual(assemble_class_lists.combine_classes(
            ['drifting_longlines', 'purse_seines', 'unknown_fishing']), 
            'drifting_longlines|purse_seines')

    def test_combine_scalars(self):
        self.assertEqual(assemble_class_lists.combine_scalars(
            [0.48, 0.5, 0.52]),
            0.5)
        self.assertEqual(assemble_class_lists.combine_scalars(
            [0.3, 0.5, 0.7]),
            None)

    def test_combine_mmsi(self):
        self.assertEqual(assemble_class_lists.combine_mmsi(
            [654, 654, 654]), 654)
        with self.assertRaises(AssertionError):
            assemble_class_lists.combine_mmsi([654, 654, 1])

    def test_combine_fields(self):
        mapping = {
            1 : VesselRecord([654, 654, 654], ['drifting_longlines', 'purse_seines', 'unknown_fishing'],
                [0.48, 0.5, 0.52], [1.9, 2.1], [], None)
        }
        self.assertEqual(assemble_class_lists.combine_fields(mapping), 
            {1: VesselRecord(mmsi=654, label='drifting_longlines|purse_seines', length=0.5, 
                engine_power=2.0, tonnage=None, split=None)})


if __name__ == '__main__':
    unittest.main()

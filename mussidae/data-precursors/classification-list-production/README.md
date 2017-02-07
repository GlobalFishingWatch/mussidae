# mussidae

These are Jupyter Notebooks developed to make training lists for the neural net. It creates lists that have tonnage (in GT), length (in meters), engine_power (in kw), and `label`, which is the vessel class (or if a fishing vessel, its geartype.)


The lables can be course or fine. The course labels are:


tug
reefer
trawlers
passenger
fixed_gear
purse_seines
squid_jigger
other_fishing
seismic_vessel
cargo_or_tanker
drifting_longlines

The fine labels are:

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

Combosite labels are:

composite_labels = {'passenger',
                    'unknown_fishing',
                    'unknown_longline',
                    'unknown_not_fishing'}  

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
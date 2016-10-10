# Lists Used by the Neural Net for Training and Testing
As of October 10, 2016, the following lists are used in the Nerual Net Classifier. Below are David's notes on what to change and use instead:

## ITU_Dec_2015_full_list.csv
 - 11774 vessels
 - It is strange that it only has the follwing categories:
```
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
    # TODO: [bitsofbits] More 
```

This pdf gives all the values that are linked http://www.itu.int/net/ITU-R/terrestrial/mars/help/table-2.pdf

This is very weird because we could be including trawlers here.


## CLAVRegistryMatchingv5.csv
 - 4803 different vessels
 - I'm skeptical of the shiptype generated in this list because I know that when I concat the geartypes, it isn't always clear what the vessel type is
 - Bjorn now has newer matches

## KnownVesselCargoTanker.csv
 - 2285 vessels
 - no idea where they came from

## KristinaManualClassification.csv
 - 2184 vessels


## PyBossaNonFishing.csv
 - 153 tug boats

## AlexWManualNonFishing.csv
 - 218 vessles that are Tugs, Passengers, Tangers, Cargo

## EUFishingVesselRegister.csv
 - 6489 vessels that are matched to the EU list, with their EU list geartype. See http://ec.europa.eu/fisheries/fleet/index.cfm?method=Codification.Cod_gear for the geartype

## PeruvianSquidFleet.csv
 - Complied by Bjorn, 104 vessels

## WorldwideSeismicVesselDatabase4Dec15.csv
 - 169 vessels, with length included



 # Lists not Used
 - rivervessels_20160502.csv
 - verify5and24_20160318.csv
 - verify5and24_20160502.csv
 - FishingVesselsV2_HighConfidenceStudents_20160502.csv

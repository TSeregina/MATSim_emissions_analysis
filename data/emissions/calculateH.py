##################################################################################
# Study: Emissions preprocessing
# Purpose: Read emissions events and save raw data per hour
# Author: Marjolaine Lannes
# Creation date: July 4, 2023
# Note: Save one emissions file per hour
# param pour config : car_fleet_is_average
# modules : time, gzip, xml.etree.ElementTree
##################################################################################
import gzip
from timeit import default_timer as timer
import datetime
# from include.emissions_events_reader import emissions_events_reader
import pickle
import os
import sys
import math
start = timer()

# Emissions events reader
def emissions_events_reader(filepath, average_fleet):
    import time, gzip
    import xml.etree.ElementTree as Xet
    with gzip.open(filepath, 'r') as f:
        tree = Xet.iterparse(f)
        _, root = next(tree)
        try :
            print(_, root)
            for event,elem in tree:
                attributes = elem.attrib
                if "type" in attributes.keys() :
                    attributes['type'] = attributes['type'][0:4] # ('cold' or 'warm')
                    if average_fleet :
                        attributes.pop('vehicleId')
                    yield attributes
                root.clear()
        except Exception as e:
            print('*** XML ERROR:', e)

# Input / output files
hour = int(sys.argv[1])
scenario_path = "F:/..."
hourly_emissions_path = scenario_path + 'cleaned_emissions/h' + str(hour) + '/'
input_dir = os.listdir(hourly_emissions_path)
N_files = len([name for name in input_dir])
output_dir = scenario_path + 'emissions_calculated_hourly/'

for i, emissions_f in enumerate(input_dir):
    # Initialize data
    output_t = output_dir + 'emissions_h' + str(hour) + '.pkl.gz'
    if os.path.exists(output_t) :
        with gzip.open(output_t, 'rb') as input_f :
            outputs = pickle.load(input_f)
    else :
        outputs = {}

    # Divide per hour
    car_fleet_is_average = True
    events = emissions_events_reader(emissions_f, average_fleet=car_fleet_is_average)
    pollutants = ['CO','CO2_TOTAL','HC','NMHC','NOx','NO2','PM','PM_non_exhaust',
                  'SO2','PM2_5','PM2_5_non_exhaust','BC_exhaust','BC_non_exhaust',
                  'Benzene','PN','CH4','NH3']
    for event in events :
        link = event['linkId']
        event_type = event['type']
        # If the link is already in the dictionary, just add the information
        if link in outputs.keys() :
            if event_type in outputs[link].keys() :
                for pollutant in pollutants:
                    if pollutant in event.keys():
                        outputs[link][event_type][pollutant] = math.fsum([outputs[link][event_type][pollutant], float(event[pollutant])])
            else :
                outputs[link][event_type] = {}
                for pollutant in pollutants:
                    if pollutant in event.keys():
                        outputs[link][event_type][pollutant] = float(event[pollutant])
                    else :
                        outputs[link][event_type][pollutant] = 0.0
        # Else, initialize a new link dict
        else :
            outputs[link] = {event_type: {}}
            for pollutant in pollutants:  # for detailed car fleet, add "car_type" (with subsegment) here
                if pollutant in event.keys():
                    outputs[link][event_type][pollutant] = float(event[pollutant])
                else:
                    outputs[link][event_type][pollutant] = 0.0

    # Save last hour dictionary
    with gzip.open(output_t, 'wb') as output_f:
        pickle.dump(outputs, output_f)
    end = timer()
    k = 100*i/N_files
    print("hour ", hour,", ", k, "% in", datetime.timedelta(seconds=end-start), " sec.")
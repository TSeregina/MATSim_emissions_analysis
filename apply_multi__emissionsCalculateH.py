# Apply:
# python apply_multi__emissionsCalculateH.py

# Indicate if run in test folder
#    [test] : if True, test folder is used, i.e. base directory with "_test" suffix
# test_add = "_test" if test else ""
test_add = "" #"_test"

import argparse
import pandas as pd
import timeit

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

def emissionsCalculateH(h, f_nb, input_dir, output_dir):
    import gzip
    import os
    import pickle
    import math
    from timeit import default_timer as timer
    import datetime

    start = timer()
    input_files = os.listdir("%s/h%d/" %(input_dir,h))

    for i, emissions_f in enumerate(input_files):
        # Initialize data
        output_t = "%s/emissions_h%d.pkl.gz" %(output_dir,h)
        if os.path.exists(output_t):
            with gzip.open(output_t, 'rb') as input_f:
                outputs = pickle.load(input_f)
        else:
            outputs = {}

        # Divide per hour
        car_fleet_is_average = True
        filepath = "%s/h%d/%s" %(input_dir,h,emissions_f)
        events = emissions_events_reader(filepath, average_fleet=car_fleet_is_average)
        pollutants = ['CO', 'CO2_TOTAL', 'HC', 'NMHC', 'NOx', 'NO2', 'PM', 'PM_non_exhaust',
                      'SO2', 'PM2_5', 'PM2_5_non_exhaust', 'BC_exhaust', 'BC_non_exhaust',
                      'Benzene', 'PN', 'CH4', 'NH3']
        for event in events:
            link = event['linkId']
            event_type = event['type']
            # If the link is already in the dictionary, just add the information
            if link in outputs.keys():
                if event_type in outputs[link].keys():
                    for pollutant in pollutants:
                        if pollutant in event.keys():
                            outputs[link][event_type][pollutant] = math.fsum(
                                [outputs[link][event_type][pollutant], float(event[pollutant])])
                else:
                    outputs[link][event_type] = {}
                    for pollutant in pollutants:
                        if pollutant in event.keys():
                            outputs[link][event_type][pollutant] = float(event[pollutant])
                        else:
                            outputs[link][event_type][pollutant] = 0.0
            # Else, initialize a new link dict
            else:
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
        k = 100 * i / f_nb
        print("hour ", h, ", ", k, "% in", datetime.timedelta(seconds=end - start), " sec.")

#========================================
# multiprocessing
#========================================
import multiprocessing as mp

# Main folder
path_base = r"F:\lvmt_TS\MATSim_Emissions_IdF\emission_average_IDF_100pct%s\60iter" % (test_add)
# Paths for input/output files
excel_path = "%s/%s" % (path_base,"emissionsF_changeH_lst.csv")
input_dir = "%s/%s" % (path_base,"emissions_cleaned")
output_dir = "%s/%s" % (path_base,"emissions_calculated_hourly")
input_fn = "emissions_average_default_vehicle"
# Number of processes by default
nb_proc = mp.cpu_count()-10

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--excel", required=False, default=excel_path, help="excel file with column: file_id")
parser.add_argument("-o", "--output", required=False, default=output_dir, help="output folder")
parser.add_argument("-p", "--processes", required=False, default=str(nb_proc),
                    help="number of processes at a time, default is number of CPU cores - 10")

args = parser.parse_args()
excel_path = args.excel
output_path = args.output
nb_proc = int(args.processes)

def job(row):
    h = row.h
    f_nb = row.file_id_end - row.file_id_start +1
    try:
        emissionsCalculateH(h, f_nb, input_dir=input_dir, output_dir=output_dir)
    except:
        print('ERROR:', h)


if __name__ == '__main__':
    time_start = timeit.default_timer()

    df = pd.read_csv(excel_path)
    df.columns = ['h','file_id','file_id_start','file_id_end']
    rows = [row for index, row in df.iterrows()]
    print(len(df))

    # ==== Multiprocessing
    from multiprocessing import Pool
    p = Pool(processes=nb_proc)
    inputs = rows
    try:
        p.map(job, inputs)
    finally:
        p.close()
        p.join()
    # ====================#

    # Time execution: Display and store in excel
    time_execution = timeit.default_timer() - time_start
    print('Done in %d seconds.' %(time_execution))

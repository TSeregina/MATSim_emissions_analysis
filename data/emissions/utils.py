#==============================
# F: Take raw emissions file with [file_id] identifier from [input_dir] in gz format,
#    clean the files to have correct xml elements without empty lines
#    and save gz into [output_dir].
#    [fn_base] : base filename, default = "emissions_average_default_vehicle"
#==============================
def emissionsClean(file_id, input_dir, output_dir, fn_base = "emissions_average_default_vehicle"):
    import gzip

    input_path = "%s/%s%d.xml.gz" % (input_dir, fn_base, file_id)
    output_path = "%s/%s%d.xml.gz" % (output_dir, fn_base, file_id)

    with gzip.open(input_path, "rb") as f_in,\
            gzip.open(output_path, 'wt') as f_out:
        # Start file
        line = '<?xml version="1.0" encoding="utf-8"?>\n'
        f_out.write(line.encode('utf8') if isinstance(line, bytes) else line)
        # Start events tag
        line = '<events version="1.0">\n'
        f_out.write(line.encode('utf8') if isinstance(line, bytes) else line)
        # Read f_in xml
        for line in f_in:
            if line.strip() and len(line) > 40:
                f_out.write(line.rstrip().decode('utf8') + "\n" if isinstance(line, bytes) else line)
        # Close events tag
        line = '</events>'
        f_out.write(line.encode('utf8') if isinstance(line, bytes) else line)

#==============================
# F: Take cleaned emissions files with [file_id] identifier from [input_dir] in gz format,
#    read first emission event and check hour of the event time;
#    identify [file_id] with event time from different hours;
#    create list of all such [file_id] and save into csv.
#==============================
def emissionsChangeH_lst(input_dir, fp_info_fhh, fn_base = "emissions_average_default_vehicle", hh_lst = []):
    import os, gzip
    from itertools import islice
    import re
    import pandas as pd

    # Number of files in the [input_dir]
    f_nb = len(os.listdir(input_dir))

    for i in range(1,f_nb+1):
        with gzip.open("%s/%s%d.xml.gz" %(input_dir,fn_base,i), "rb") as f_in:
            # Read only line 3, i.e. the 1st emission event
            for line in islice(f_in,2,3):
                # Extract hour of the 1st emission event in the file
                line_h = int( re.search(r'\d+',str(line)).group() ) // 3600
                # Collect [file_id] containing 2 different hours into [hh_lst]
                # Once new hour occurs, the previous [file_id] is collected into the list [hh_lst],
                # i.e. the list index corresponds to each new hour + 3;
                # [+3] since the 1st hour of emissions events in the simulation is usually 3.
                if len(hh_lst) < line_h-3:
                    hh_lst.append(i-1)
    hh_dict = {'h':list(range(3,len(hh_lst)+3)), 'file_id':hh_lst}
    df = pd.DataFrame(hh_dict)
    df.to_csv(fp_info_fhh, index=False)
    return hh_lst

#==============================
# F: Take emission file that contain events of different hours,
#    move new hour events from such a file to the next file,
#    s.t. each resulting file does not contain events of different hours.
#==============================
def emissionsDivideChangeH(h, file_id, input_dir, output_dir, fn_base):
    import gzip
    import re
    import os

    fp_in1 = "%s/%s%d.xml.gz" %(input_dir,fn_base,file_id)
    fp_in2 = "%s/%s%d.xml.gz" %(input_dir,fn_base,file_id+1)
    fp_out1 = "%s/tmp%d.xml.gz" % (output_dir,file_id)
    fp_out2 = "%s/tmp%d.xml.gz" % (output_dir,file_id+1)

    with gzip.open(fp_in1, "rb") as f_in1, gzip.open(fp_in2, "rb") as f_in2,\
            gzip.open(fp_out1, "wt") as f_out1, gzip.open(fp_out2, "wt") as f_out2:

        # Start files tmp1 & tmp2
        line = '<?xml version="1.0" encoding="utf-8"?>\n'
        f_out1.write(line.encode('utf8') if isinstance(line, bytes) else line)
        f_out2.write(line.encode('utf8') if isinstance(line, bytes) else line)
        # Start events tag
        line = '<events version="1.0">\n'
        f_out1.write(line.encode('utf8') if isinstance(line, bytes) else line)
        f_out2.write(line.encode('utf8') if isinstance(line, bytes) else line)
        # Read f_in1 xml
        for line in f_in1:
            if line.strip() and len(line) > 40:
                if int( re.search(r'\d+',str(line)).group() ) // 3600 == h:
                    f_out1.write(line.rstrip().decode('utf8') + "\n" if isinstance(line, bytes) else line)
                else:
                    f_out2.write(line.rstrip().decode('utf8') + "\n" if isinstance(line, bytes) else line)
        # Read f_in2 xml
        for line in f_in2:
            if line.strip() and len(line) > 40:
                f_out2.write(line.rstrip().decode('utf8') + "\n" if isinstance(line, bytes) else line)
        # Close events tag
        line = '</events>'
        f_out1.write(line.encode('utf8') if isinstance(line, bytes) else line)
        f_out2.write(line.encode('utf8') if isinstance(line, bytes) else line)

    # Delete files f_in1 & f_in2
    if os.path.isfile(fp_in1):
        os.remove(fp_in1)
        os.rename(fp_out1,fp_in1)
    else:
        print("Error: %s file not found" % fp_in1)
    if os.path.isfile(fp_in2):
        os.remove(fp_in2)
        os.rename(fp_out2, fp_in2)
    else:
        print("Error: %s file not found" % fp_in2)

#==============================
# F: Take all the emissions files with events type of the hour [h],
#    move them to the created folder of the corresponding hour.
#==============================
def emissionsDivideFolderH(h, file_id_start, file_id_end, input_dir, fn_base = "emissions_average_default_vehicle"):
    import os

    dir_h = "h%d" % h
    os.mkdir(os.path.join(input_dir, dir_h))

    for file_id in range(file_id_start,file_id_end+1):
        fp_in = "%s/%s%d.xml.gz" %(input_dir,fn_base,file_id)
        fp_out = "%s/%s/%s%d.xml.gz" %(input_dir,dir_h,fn_base,file_id)
        os.rename(fp_in,fp_out)


#==============================
# F: Create dictionary from emission file,
#    calculate emissions per type {cold, warm, pollutant} per link.
#==============================
##################################################################################
# Study: Emissions preprocessing
# Purpose: Read emissions events and save raw data per hour
# Author: Marjolaine Lannes
# Creation date: July 4, 2023
# Note: Save one emissions file per hour
# param pour config : car_fleet_is_average (average_fleet=True)
# modules : time, gzip, xml.etree.ElementTree
##################################################################################
# Emissions events reader
def emissions_events_reader(fp_emissions, average_fleet=True):
    import gzip
    import xml.etree.ElementTree as ET
    with gzip.open(fp_emissions, 'r') as f:
        tree = ET.iterparse(f)
        _, root = next(tree)
        try:
            # print(_, root)
            for event,elem in tree:
                attributes = elem.attrib
                if "type" in attributes.keys() :
                    attributes['type'] = attributes['type'][0:4] # ('cold' or 'warm')
                    if average_fleet:
                        attributes.pop('vehicleId')
                    yield attributes
                root.clear()
        except Exception as e:
            print(F"XML ERROR for {fp_emissions=}: {e=}, {type(e)=}")

# Summation of emissions by type for each link per hour.
def emissionsSumH(h, input_dir, output_dir, pollutants, average_fleet):
    import gzip
    import os
    import pickle
    import math
    from timeit import default_timer as timer
    import datetime

    start = timer()
    input_files = os.listdir(input_dir)

    for i, emissions_f in enumerate(input_files):
        # Initialize data
        output_t = "%s/emissions_h%d.pkl.gz" %(output_dir,h)
        if os.path.exists(output_t):
            with gzip.open(output_t, 'rb') as input_f:
                outputs = pickle.load(input_f)
        else:
            outputs = {}

        fp_emissions = "%s/%s" %(input_dir,emissions_f)
        events = emissions_events_reader(fp_emissions, average_fleet)
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
        k = 100 * i / len(input_files)
        print("hour ", h, ", ", k, "% in", datetime.timedelta(seconds=end - start), " sec.")
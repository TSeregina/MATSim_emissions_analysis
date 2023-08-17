"""
attributes.py
"""

import os
import multiprocessing as mp


#==============================
# (Default) test indicator (""); if (="_test"), test folder is used, i.e. with "_test" suffix
#==============================
test_add = "_test" # test_add = "_test"

# (Default) Main input/output local folders
input_dir_local = r"G:\lvmt_mlannes\simulation_Biao"
output_dir_local = r"F:\lvmt_TS\MATSim_Emissions_IdF"
# If test_add ="_test", then input_dir_local & output_dir_local are the same
if test_add:
    input_dir_local = output_dir_local

# (Default) Working directory, including test_add suffix
# to put input and create output directories.
work_dir = r"emission_average_IDF_100pct\60iter" + test_add


# Stage 1: Break
#==============================
# (Default) Path of simulation output with emissions.xml.gz
#==============================
simout_emissions_dir = r"emissions.xml.gz"
simout_emissions_fp = simout_emissions_dir + "emissions.xml.gz"
extract_event_types = ["coldEmissionEvent","warmEmissionEvent"]
dir_emissionsBreak = "%s/%s/%s" % (input_dir_local,work_dir,"emissions_breaked")

# Create output [dir_emissionsBreak] directory if doesn't exist
if not os.path.exists(dir_emissionsBreak):
    os.makedirs(dir_emissionsBreak)
    print("The output directory is created: %s" % dir_emissionsBreak)


# Stage 2: Clean
#==============================
# csv file with information on input files
fp_info_f = "%s/%s/%s" % (input_dir_local,work_dir,"info_f.csv")

# Path for output files
dir_emissionsClean = "%s/%s/%s" % (output_dir_local,work_dir,"emissions_cleaned")
# File name of input emissions files
fn_base = "emissions_average_default_vehicle"

# Specify number of emissions events per split file
break_after = 10

# Create output [dir_emissionsClean] directory if doesn't exist
if not os.path.exists(dir_emissionsClean):
    os.makedirs(dir_emissionsClean)
    print("The output directory is created: %s" % dir_emissionsClean)


# Stage 3: DivideChangeH
#==============================
# csv file with information on input files
fp_info_fhh = "%s/%s/%s" % (input_dir_local,work_dir,"info_fhh.csv")
fp_info_fh = "%s/%s/%s" % (input_dir_local,work_dir,"info_fh.csv")


# Stage 4: SumH
#==============================
# Path for output files
dir_emissionsSum = "%s/%s/%s" % (output_dir_local,work_dir,"emissions_sum")

average_fleet = True
pollutants = ['CO', 'CO2_TOTAL', 'HC', 'NMHC', 'NOx', 'NO2', 'PM', 'PM_non_exhaust',
              'SO2', 'PM2_5', 'PM2_5_non_exhaust', 'BC_exhaust', 'BC_non_exhaust',
              'Benzene', 'PN', 'CH4', 'NH3']

# Create output [dir_emissionsSumF] directory if doesn't exist
if not os.path.exists(dir_emissionsSum):
    os.makedirs(dir_emissionsSum)
    print("The output directory is created: %s" % dir_emissionsSum)


# (Default) Number of CPU = 80% of the number of cores
nb_proc = round(mp.cpu_count()*0.8)

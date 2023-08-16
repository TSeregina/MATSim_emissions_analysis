"""
This stage parallelizes the summation of emissions by type for each link per hour.
Parallelization across hours, i.e. 27 hours.
The resulting data is saved for each hour as a pickle file and organized into a dictionary:
    1st level key - linkId
    2nd level key - {cold, warm}
    values - hourly emissions sums by pollutant type.

Previous stage: emissionsDivideFolderH (parallelized)
Next stage: ... (parallelized)
"""
#==============================
# Parallelization: emissionsSumF
#
# Run:
# python apply_multi__emissionsSumH.py
#==============================
# Default arguments are loaded from attributes.py
#    [test_add] - indicates if run in test directory; default ""; if (="_test"), test directory is used
#    [dir_emissionsClean] - input directory
#    [dir_emissionsSum] - output directory
#    [pollutants] - list of pollutants
#    [average_fleet] - (default True) indicates if average car fleet was assumed for calculations
#    [fp_info_fh] - csv file with information on input hour folders
#    [nb_proc] - number of processes, default = 80% of the number of cores
#==============================

import pandas as pd
import timeit
import multiprocessing as mp

from data.emissions.utils import emissionsSumH
import attributes as attr

#==============================
# Define job per process
#==============================
def job(row):
    h = row.h
    dir_h = "h%d" % h
    try:
        emissionsSumH(h,
                      input_dir = "%s/%s" %(attr.dir_emissionsClean,dir_h),
                      output_dir = attr.dir_emissionsSum,
                      pollutants = attr.pollutants,
                      average_fleet = attr.average_fleet)
    except Exception as e:
        print(F"ERROR for {h=}: {e=}, {type(e)=}")


if __name__ == '__main__':
    # Start timer (all processes)
    time_start = timeit.default_timer()

    # Read info (file_id, ...) on files to be processed
    df = pd.read_csv(attr.fp_info_fh,header=0)
    rows = [row for index, row in df.iterrows()]
    print("%d directories for multiprocessing." % len(df))
    print("%d processes reserved from maximum of %d." % (attr.nb_proc, mp.cpu_count()))

    #==== Multiprocessing
    from multiprocessing import Pool
    p = Pool(processes=attr.nb_proc)
    inputs = rows
    try:
        p.map(job, inputs)
    finally:
        p.close()
        p.join()
    #====================#

    # Time execution: Display
    print('Done in %d seconds.' % (timeit.default_timer() - time_start))

    # h_dict = pd.read_pickle(r"F:\lvmt_TS\MATSim_Emissions_IdF\emission_average_IDF_100pct\60iter_test\emissions_sum\emissions_h3.pkl.gz")

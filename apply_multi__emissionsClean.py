"""
This stage parallelize cleaning of split emissions files with emissions events only.

Previous stage: emissionsBreak
Next stage: emissionsDivideChangeH (parallelized)
"""
#==============================
# Parallelization: emissionsClean
#
# Run:
# python apply_multi__emissionsClean.py
#==============================
# Default arguments are loaded from attributes.py
#     [test_add] - indicates if run in test directory; default ""; if (="_test"), test directory is used
#     [input_dir_local] & [output_dir_local] - main input & output local directories
#     [fp_info_f] - csv file with information on files
#     [nb_proc] - number of processes, default = 80% of the number of cores
#==============================

import pandas as pd
import timeit
import multiprocessing as mp

from data.emissions.utils import emissionsClean
import attributes as attr

#==============================
# Define job per process
#==============================
def job(row):
    file_id = row.file_id
    try:
        emissionsClean(file_id,
                       input_dir = attr.dir_emissionsBreak,
                       output_dir = attr.dir_emissionsClean,
                       fn_base = attr.fn_base)
    except Exception as e:
        # print(e)
        print(F"ERROR for {file_id=}: {e=}, {type(e)=}")


if __name__ == '__main__':
    # Start timer (all processes)
    time_start = timeit.default_timer()

    # Read info (file_id, ...) on files to be processed
    df = pd.read_csv(attr.fp_info_f,header=0)
    rows = [row for index, row in df.iterrows()]
    print("%d files for multiprocessing." % len(df))
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

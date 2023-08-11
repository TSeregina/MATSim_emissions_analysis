"""
This stage takes split emissions files,
identify those files that contain events with time of different hours,
takes new hour events from such a file and moves them to the next file,
s.t. each resulting file does not contain events of different hours

Previous stage: emissionsClean (parallelized)
Next stage: emissionsDivideFolderH (parallelized)
"""
#==============================
# Parallelization: emissionsDivideChangeH
#
# Run:
# python apply_multi__emissionsDivideChangeH.py
#==============================
# Default arguments are loaded from attributes.py
#==============================
import pandas as pd
import timeit
import multiprocessing as mp

from data.emissions.utils import emissionsChangeH_lst, emissionsDivideChangeH
import attributes as attr

#==============================
# Define job per process
#==============================
def job(row):
    h = row.h
    file_id = row.file_id
    try:
        emissionsDivideChangeH(h, file_id,
                               input_dir = attr.dir_emissionsClean,
                               output_dir = attr.dir_emissionsClean,
                               fn_base = attr.fn_base)

    except Exception as e:
        print(F"ERROR for {file_id=}: {e=}, {type(e)=}")


if __name__ == '__main__':
    # Start timer (all processes)
    time_start = timeit.default_timer()

    # Create a list of [file_id] that contain events of different hours
    # and save it into fp_info_fhh.csv.
    hh_lst = emissionsChangeH_lst(input_dir = attr.dir_emissionsClean,
                                  fp_info_fhh = attr.fp_info_fhh,
                                  fn_base = attr.fn_base,
                                  hh_lst=[])

    # Read info (file_id, ...) on files to be processed
    df = pd.read_csv(attr.fp_info_fhh,header=0)
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

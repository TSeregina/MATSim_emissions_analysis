"""
This stage moves split emissions files to the folder of corresponding hour.

Previous stage: emissionsDivideChangeH (parallelized)
Next stage: emissionsCalculateH (parallelized per hour)
"""
#==============================
# Parallelization: emissionsDivideFolderH
#
# Run:
# python apply_multi__emissionsDivideFolderH.py
#==============================
# Default arguments are loaded from attributes.py
#==============================
import pandas as pd
import os
import timeit
import multiprocessing as mp

from data.emissions.utils import emissionsDivideFolderH
import attributes as attr

#==============================
# Define job per process
#==============================
def job(row):
    h = row.h
    file_id_start = row.file_id_start
    file_id_end = row.file_id_end
    try:
        emissionsDivideFolderH(h, file_id_start, file_id_end,
                               input_dir=attr.dir_emissionsClean,
                               fn_base = attr.fn_base)
    except Exception as e:
        print(F"ERROR for {h=}: {e=}, {type(e)=}")


if __name__ == '__main__':
    # Start timer (all processes)
    time_start = timeit.default_timer()

    # Read info (file_id, ...) on files to be processed
    df = pd.read_csv(attr.fp_info_fhh,header=0)

    # Number of all emissions files - to know the files for the last hour directory
    f_nb = len(os.listdir(attr.dir_emissionsClean))

    # Add columns ['file_id_start'] and ['file_id_end']
    df.loc[len(df.index)] = [len(df.index)+3, f_nb]
    df['file_id_start'] = df['file_id'].shift(1)+1
    df.loc[0,'file_id_start'] = 1
    df['file_id_end'] = df['file_id']

    df['file_id_start'] = df['file_id_start'].astype('int')
    df['file_id_end'] = df['file_id_end'].astype('int')

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

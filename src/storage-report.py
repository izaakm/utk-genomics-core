#!/usr/bin/env python3
# coding: utf-8

'''
Storage Report for Genomics Core Facility (gensvc)
==================================================

...

Roadmap
=======

- Include gensvc/archive in the report
- Include lfs quota in the report
- Generate a summary report (--summary) kind of like this:

        Gensvc storage report
        144 Sequencing runs (including a handful of test runs)
        34 TB of raw data
        Average run size: about 230 GB
        28 Archived runs
        6.4 TB total
        Average archive size: about 228 GB
        lfs quota: 52.5 TB used by gensvc user, 73.9 TB used by project UTK0192
'''

import os
import pathlib
import re
import subprocess
import time
import json
import sys

# import pandas as pd

# This should be from env variable, eg, GENSVC_DATADIR
# DATADIR = pathlib.Path('/Users/johnmiller/data/gensvc').expanduser()
GENSVC_DATADIR = pathlib.Path(os.getenv('GENSVC_DATADIR', ''))

tb = 1024**4
block_size = 1024
reg_seqrun = re.compile(r'\d{6}[\w-]+')

def get_rundirs(path):
    yes = []
    no = []
    for d in path.glob('*'):
        if reg_seqrun.search(str(d)):
            yes.append(d)
        else:
            no.append(d)
    return yes


def proc_du(s):
    try:
        blocks, path = s.strip().split()
        blocks = int(blocks)
        bytes_ = blocks * block_size
        return {'blocks': blocks, 'bytes': bytes_, 'path': path}
    except ValueError:
        print(f"Error processing: {s}", file=sys.stderr)
        return {}

def main():

    data = {}
    data['counts'] = {
        'total': 0,
    }
    data['storage'] = {
        'total_bytes': 0,
        'total_blocks': 0,
        'rundirs': []
    }

    seqrun_dirs = GENSVC_DATADIR.glob('*Runs')
    # miseqruns = pathlib.Path('/Users/johnmiller/data/gensvc/MiSeqRuns')
    # novaseqruns = pathlib.Path('/Users/johnmiller/data/gensvc/NovaSeqRuns')

    procs = []
    # stdout = []
    total_runs = 0
    # total_storage = 0
    total_bytes = 0
    total_blocks = 0
    for seq_inst in seqrun_dirs:
        print('Processing:', seq_inst, file=sys.stderr)
        rundirs = get_rundirs(seq_inst)
        total_runs += len(rundirs)

        # Update the data:
        data['counts'][str(seq_inst)] = len(rundirs)

        procs = []
        for d in rundirs:
            proc = subprocess.Popen(['du', '-B', str(block_size), '-s', str(d)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            procs.append(proc)

        # print('submitted jobs.')

        # print('Wait for the processes to finish')
        # This could be moved outside of this loop, but then I get "OSError: [Errno 24] Too many open files"
        for proc in procs:
            o, e = proc.communicate()
            res = proc_du(o.decode())
            total_bytes += res['bytes']
            total_blocks += res['blocks']
            data['storage']['rundirs'].append(res)
            # print(f"Process {proc.pid} finished with code {proc.returncode}")
        # print('All complete')


    data['counts']['total'] = total_runs
    data['storage']['total_bytes'] = total_bytes
    data['storage']['total_blocks'] = total_blocks
    
    print(json.dumps(data, indent=2, sort_keys=True))

    # df = pd.DataFrame([o.strip().split() for o in stdout], columns=['blocks', 'path'])
    # df['blocks'] = df['blocks'].astype(int)
    # df['bytes'] = df['blocks'] * 1024
    # df['bytes'].sum() / tb

    return 0

if __name__ == '__main__':
    sys.exit(main())

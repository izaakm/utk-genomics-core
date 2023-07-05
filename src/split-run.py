#!/usr/bin/env python
# coding: utf-8

import argparse
import hashlib
import json
import pandas as pd
import pathlib
import re
import shutil
import sys
import os

from sample_sheet import SampleSheet

NONALPHANUMERIC = re.compile('[^A-Za-z0-9]+')

CHECKSUMS_SEP = '  '

# PROJECTDIR = pathlib.Path().home() / 'projects/gensvc'
# PROJECTDIR.exists()

# DATADIR = pathlib.Path().home() / 'data'
# DATADIR.exists()

DATADIR = os.getenv('DATADIR') or pathlib.Path().home() / 'data'

def shasum(path, buf_size=65536):
    # # BUF_SIZE is totally arbitrary, change for your app!
    # BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    
    # md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    
    with open(path, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            # md5.update(data)
            sha1.update(data)
    
    # print("MD5: {0}".format(md5.hexdigest()))
    # print("SHA1: {0}".format(sha1.hexdigest()))

    return sha1.hexdigest()

def main():

    def _path(x):
        if not isinstance(x, pathlib.Path):
            x = pathlib.Path(x)
        x = x.expanduser()
        return x

    parser = argparse.ArgumentParser(
        prog='Gensvc Illumina handler',
        description='Sort and process Illumina Data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # run_dir = DATADIR / 'MiSeqRuns/230630_M04398_0054_000000000-L63FY'
    parser.add_argument(
        'run_dir',
        type=_path,
        help="Path to Illumina run"
    )
    parser.add_argument(
        '-d', '--datadir',
        default=DATADIR,
        type=_path,
        help="Path to project directory, eg 'projects/gensvc'"
    )
    # parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag
    parser.add_argument('--debug', action='store_true')  # on/off flag

    args = parser.parse_args()

    path_to_samplesheet = args.run_dir / 'SampleSheet.csv'

    errors = 0

    if args.debug:
        print(args)

    if not args.run_dir.exists():
        errors += 1
        print(f'[ERROR] run_dir does not exist: {str(args.run_dir)}')

    if not args.datadir.exists():
        errors += 1
        print(f'[ERROR] --datadir does not exist: {str(args.datadir)}')

    if not path_to_samplesheet.exists():
        errors += 1
        print(f'[ERROR] path_to_samplesheet does not exist: {str(path_to_samplesheet)}')

    fastq_dir = list(args.run_dir.rglob('Fastq'))
    if len(fastq_dir) > 1:
        errors += 1
        print(f'[ERROR] There can be only one fastq directory: {fastq_dir}')
    else:
        fastq_dir = fastq_dir[0]

    if not fastq_dir.exists():
        errors += 1
        print(f'[ERROR] fastq_dir does not exist: {str(fastq_dir)}')

    if errors > 0:
        sys.exit(1)

    checksums_filepath = args.datadir / 'checksums'

    outdir = args.datadir / 'processed'
    outdir.mkdir(parents=True, exist_ok=True)

    sample_sheet = SampleSheet(path_to_samplesheet)

    df = pd.DataFrame([_sample.to_json() for _sample in sample_sheet])

    # view = df.query('Sample_Project=="UTK_bbruce_Bruce_230630"').copy()
    # _table = view

    for idx, row in df.iterrows():
        print('---')
        # print(row)
        # print(row['Sample_Project'])
        # print(row['Sample_ID'])
        sample_prefix = NONALPHANUMERIC.sub('-', row['Sample_ID'])
        # print(sample_prefix)
        filelist = list(fastq_dir.rglob(f'{sample_prefix}*.fastq*'))
        # print(*filelist, sep='\n')
        for source_path in filelist:
            dest_path = outdir / args.run_dir.name / row['Sample_Project'] / source_path.name

            print('source: ', source_path)
            print('dest  : ', dest_path)

            # [TODO] Prompt to copy [???]
            response = input('Do you want to copy "source" to "dest" (above): [Y/n/q] ')
            response = response.lower()
            if response.startswith('q'):
                print('Quitting')
                return 1
            elif response.startswith('y'):
                # print(source_path, '->', dest_path)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, dest_path)
                
                source_shasum = shasum(source_path)
                dest_shasum = shasum(dest_path)
                print(source_shasum, source_path.relative_to(args.datadir), sep=CHECKSUMS_SEP)
                print(dest_shasum, dest_path.relative_to(args.datadir), sep=CHECKSUMS_SEP)

                if not source_shasum == dest_shasum:
                    print('[WARNING] Checksums do not match (see above).')
                
                with open(checksums_filepath, 'a') as f:
                    print(dest_shasum, dest_path.relative_to(args.datadir), sep=CHECKSUMS_SEP, file=f)
            else:
                print('*** SKIPPED ***')
    return 0


if __name__ == '__main__':
    main()
    sys.exit(0)

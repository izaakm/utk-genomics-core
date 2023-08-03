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


def get_samples(sample_sheet):
    return pd.DataFrame([_sample.to_json() for _sample in sample_sheet])


def display_unique(sample_sheet, columns=None):
    df = get_samples(sample_sheet)
    view = df[columns].drop_duplicates().sort_values(columns)
    print(view.to_string(index=False))


def check_samples(sample_sheet, fastq_dir, verbose=False, debug=False):

    df = get_samples(sample_sheet)

    fastq_files = list(fastq_dir.rglob(f'*.fastq*'))
    # print(*fastq_files, sep='\n')
    for sample_project, view in df.groupby('Sample_Project'):
        for idx, row in view.iterrows():
            print('---')
            # print(row)
            # sample_prefix = NONALPHANUMERIC.sub('-', row['Sample_ID'])
            # filelist = [f for f in fastq_files if sample_prefix in f.name]
            # print(f'sample_prefix = {sample_prefix}')

            # Change the search string because demultiplexing is slightly different on NovaSeq vs MiSeq:
            # - NovaSeq uses bcl2fastq: keep underscores in `Sample_ID` as underscores in filename
            # - MiSeq does not use bcl2fastq: change underscores in `Sample_ID` to dashes in filename
            # Solution: change dashes and underscores in `Sample_ID` to a regex `[-_]` to match dash or underscore in filename.
            sample_query = re.sub('[-_]', r'[-_]', row['Sample_ID'])
            if debug:
                print(row['Sample_ID'], '->', sample_query)
            filelist = [f for f in fastq_files if re.search(sample_query, f.name)]
            if verbose:
                for key in ['Sample_Project', 'Sample_ID']:
                    if key in row:
                        long_message += f'{key} : {row[key]}\n'
                if filelist:
                    print('[OK] Found files:')
                    print(*filelist, sep='\n')
                else:
                    print(f'[MISSING] No FASTQ files matching `{sample_query}`')
            else:
                if filelist:
                    print(f'[OK] {row["Sample_Project"]} , {row["Sample_ID"]}')
                else:
                    print(f'[MISSING] {row["Sample_Project"]} , {row["Sample_ID"]}')

        print('#'*72)


def main():

    def _path(x):
        if isinstance(x, pathlib.Path):
            x = x.expanduser()
        elif isinstance(x, str):
            x = pathlib.Path(x)
            x = x.expanduser()
        else:
            x = None
        return x

    parser = argparse.ArgumentParser(
        prog='Gensvc Illumina handler',
        description='Sort and process Illumina Data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-r', '--run-dir',
        type=_path,
        help="Path to Illumina run"
    )

    parser.add_argument(
        '-s', '--samplesheet',
        type=_path,
        help="Path to Sample Sheet"
    )

    parser.add_argument(
        '-f', '--fastq-dir',
        type=_path,
        help="Path to directory with FASTQ files (e.g., the `--outdir` from `bcl2fastq`)"
    )

    parser.add_argument(
        '-u', '--unique',
        type=str,
        nargs='*',
        help="Display unique values for the provided fields"
    )

    parser.add_argument(
        '-c', '--check-samples',
        type=str,
        nargs='*',
        help="Check for FASTQ files corresponding to samples in the sample sheet"
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help="Print info about the samples and matching files; otherwise, just print OK or MISSING"
    )

    parser.add_argument(
        '--debug',
        action='store_true'
    )

    args = parser.parse_args()

    if args.samplesheet:
        path_to_samplesheet = args.samplesheet
    else:
        path_to_samplesheet = args.run_dir / 'SampleSheet.csv'

    if args.debug:
        print(args)

    sample_sheet = SampleSheet(path_to_samplesheet)

    if args.unique:
        display_unique(sample_sheet, columns=args.unique)

    if args.check_samples:
        check_samples(sample_sheet, args.fastq_dir, verbose=args.verbose)

    # df = pd.DataFrame([_sample.to_json() for _sample in sample_sheet])
    # fastq_files = list(args.fastq_dir.rglob(f'*.fastq*'))
    # # print(*fastq_files, sep='\n')
    # for sample_project, view in df.groupby('Sample_Project'):
    #     for idx, row in view.iterrows():
    #         print('---')
    #         # print(row)
    #         # sample_prefix = NONALPHANUMERIC.sub('-', row['Sample_ID'])
    #         # filelist = [f for f in fastq_files if sample_prefix in f.name]
    #         # print(f'sample_prefix = {sample_prefix}')
    #         # Change the search string because demultiplexing is slightly different on NovaSeq vs MiSeq:
    #         # - NovaSeq uses bcl2fastq: keep underscores in `Sample_ID` as underscores in filename
    #         # - MiSeq does not use bcl2fastq: change underscores in `Sample_ID` to dashes in filename
    #         # Solution: change dashes and underscores in `Sample_ID` to a regex `[-_]` to match dash or underscore in filename.
    #         sample_query = re.sub('[-_]', r'[-_]', row['Sample_ID'])
    #         if args.debug:
    #             print(row['Sample_ID'], '->', sample_query)
    #         filelist = [f for f in fastq_files if re.search(sample_query, f.name)]
    #         if args.verbose:
    #             for key in ['Sample_Project', 'Sample_ID']:
    #                 if key in row:
    #                     long_message += f'{key} : {row[key]}\n'
    #             if filelist:
    #                 print('[OK] Found files:')
    #                 print(*filelist, sep='\n')
    #             else:
    #                 print(f'[MISSING] No FASTQ files matching `{sample_query}`')
    #         else:
    #             if filelist:
    #                 print(f'[OK] {row["Sample_Project"]} , {row["Sample_ID"]}')
    #             else:
    #                 print(f'[MISSING] {row["Sample_Project"]} , {row["Sample_ID"]}')
    #     print('#'*72)

    return 0


if __name__ == '__main__':
    main()
    sys.exit(0)

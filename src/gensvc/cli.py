#!/usr/bin/env python
# coding: utf-8

'''
Helper for working with data from the Genomics Core.
'''

# Python standard library
import pathlib
import argparse
import sys
import os
import tempfile

from gensvc.wrappers import bcl2fastq, slurm
from gensvc.core_facility import reports, transfer
from gensvc.data import sequencing_run
from gensvc.misc import config, utils

# def as_path(obj):
#     try:
#         return pathlib.Path(obj)
#     except:
#         return None

# GENSVC_DATADIR = as_path(os.getenv('GENSVC_DATADIR'))
#
# GENSVC_MISEQ_DATADIR = as_path(os.getenv('GENSVC_MISEQ_DATADIR'))
# GENSVC_NEXTSEQ_DATADIR = as_path(os.getenv('GENSVC_NEXTSEQ_DATADIR'))
# GENSVC_NOVASEQ_DATADIR = as_path(os.getenv('GENSVC_NOVASEQ_DATADIR'))
#
# GENSVC_PROCDATA = as_path(os.getenv('GENSVC_PROCDATA'))


def run_list(args):
    for d in args.dirs:
        reports.list(d, long=args.long)
    return 0


def run_bcl2fastq(args):
    print(f'GENSVC_PROCDATA={GENSVC_PROCDATA}')
    seqrun = sequencing_run.IlluminaSequencingData(args.runfolder_dir)
    # print(seqrun.info)
    # seqrun.init_procdir()

    # Do not use os.cpu_count() with sbatch.
    # TODO Have the batch script get the number of cpus from the slurm environment.
    command = bcl2fastq.bcl2fastq(
        runfolder_dir=seqrun.realpath,
        sample_sheet=args.sample_sheet or seqrun.path_to_samplesheet,
        output_dir=args.output_dir or bcl2fastq.init_output_dir(GENSVC_PROCDATA, seqrun.runid),
        processing_threads=args.processing_threads
    )
    if args.sbatch:
        batch = slurm.Slurm(**slurm.default_kwargs)
        print(batch)
        print(command)
    else:
        print(command)


def extract_bcl2fastq_stats(args):
    tables = bcl2fastq.extract_stats(args.bcl2fastq_dir)
    bcl2fastq.write_summary_stats(tables, args.outdir, dry_run=args.dry_run)
    return 0


def run_setup_transfer(args):
    procdata = sequencing_run.ProcessedData(path=args.dirname)
    print(procdata)

    transfer.setup_transfer(
        procdir=procdata.path,
        dry_run=args.dry_run
    )

def run_transfer(args):
    pass

def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        default=False,
        help='Just print the output.'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='Print extra stuff.'
    )

    subparsers = parser.add_subparsers(help='sub-command help')

    # Generate summary stats.
    parse_extract_bcl2fastq_stats = subparsers.add_parser(
        'extract-bcl2fastq-stats', 
        aliases=['ex'],
        help=bcl2fastq.__doc__.strip().split('\n')[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parse_extract_bcl2fastq_stats.add_argument(
        'bcl2fastq_dir',  # Must be underscore.
        action='store',
        type=pathlib.Path,
        help='The bcl2fastq output directory containing the "Reports" and "Stats" subdirectories.'
    )

    parse_extract_bcl2fastq_stats.add_argument(
        '--statsfile',
        action='store',
        type=pathlib.Path,
        help=(
            'Optional: Statistics json file generated by bcl2fastq, eg '
            '"<bcl2fastq output>/Stats/Stats.json". If --fastqdir is '
            'provided, this defaults to "<fastqdir>/Stats/Stats.json".'
        )
    )

    parse_extract_bcl2fastq_stats.add_argument(
        '--outdir',
        action='store',
        default='SummaryStatistics',
        type=pathlib.Path,
        help='Directory in which to put the output files.'
    )

    parse_extract_bcl2fastq_stats.set_defaults(func=extract_bcl2fastq_stats)

    # List sequencing runs.
    parse_reports = subparsers.add_parser(
        'list', 
        aliases=['ls', 'scan', 'sc'],
        help='List sequencing runs in the given director',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parse_reports.add_argument(
        'dirs',  
        type=pathlib.Path,
        default=[GENSVC_NOVASEQ_DATADIR],
        nargs='*',
        help='One or more directories to list.'
    )

    parse_reports.add_argument(
        '-l', '--long',
        action='store_true',
        help='List more stuff.'
    )

    parse_reports.set_defaults(func=run_list)

    # Convert BCL files to FASTQ.
    parse_converter = subparsers.add_parser(
        'convert', 
        aliases=['co'],
        help='Convert BCL files to FASTQ using Illumina\'s bcl2fastq.'
    )

    parse_converter.add_argument(
        '-r', '--runfolder-dir',
        action='store',
        type=pathlib.Path,
        help='Path to sequencing run.'
    )

    parse_converter.add_argument(
        '-s', '--sample-sheet',  
        action='store',
        type=pathlib.Path,
        help='Path to sample sheet.'
    )

    parse_converter.add_argument(
        '-o', '--output-dir',  
        action='store',
        type=pathlib.Path,
        help='Path to output directory.'
    )

    parse_converter.add_argument(
        '-t', '--processing-threads',  
        action='store',
        type=int,
        help='Number of threads to use.'
    )

    parse_converter.add_argument(
        '-b', '--sbatch',  
        action='store_true',
        help='Submit bcl2fastq job to Slurm using `sbatch`.'
    )

    parse_converter.set_defaults(func=run_bcl2fastq)

    # Trasfer data to user's project directory.
    parse_setup_transfer = subparsers.add_parser(
        'setup_transfer', 
        aliases=['se'],
        help='Set up data for tranfer.'
    )

    parse_setup_transfer.add_argument(
        'dirname',  
        action='store',
        type=str,
        help='The path to the processed data directory.'
    )

    parse_setup_transfer.add_argument(
        '-s', '--sbatch',  
        action='store_true',
        help='Run transfer as a Slurm job.'
    )

    parse_setup_transfer.set_defaults(func=run_setup_transfer)

    # Trasfer data to user's project directory.
    parse_transfer = subparsers.add_parser(
        'transfer', 
        aliases=['tr'],
        help='Transfer results to project directory.'
    )

    parse_transfer.add_argument(
        'runid',  
        action='store',
        type=str,
        help='The <runid> of the sequencing run.'
    )

    parse_transfer.add_argument(
        '-f', '--from',  
        dest='source',
        action='store',
        type=pathlib.Path,
        help='Path to transfer source directory.'
    )

    parse_transfer.add_argument(
        '-t', '--to',  
        dest='destination',
        action='store',
        type=pathlib.Path,
        help='Path to transfer destination directory.'
    )

    parse_transfer.add_argument(
        '-s', '--sbatch',  
        action='store_true',
        help='Run transfer as a Slurm job.'
    )

    parse_transfer.set_defaults(func=run_transfer)

    return parser

def main():

    args = get_parser().parse_args()
    if args.verbose:
        print(args)

    res = args.func(args)

    return res


if __name__ == '__main__':
    res = main()
    sys.exit(res)

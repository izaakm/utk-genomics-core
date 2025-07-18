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
import logging
import pandas as pd

from gensvc.wrappers import bcl2fastq, slurm
from gensvc.core_facility import reports, transfer
from gensvc.misc import config, utils
from gensvc.data import illumina

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


logger = logging.getLogger(__name__)


def cli_sample_sheet(args):
    '''
    Generate a sample sheet for BCL-Convert.
    '''
    from gensvc.data import illumina

    if args.src_sample_sheet:
        sample_sheet = illumina.read_sample_sheet(args.src_sample_sheet)
    elif args.format == 'v1':
        sample_sheet = illumina.SampleSheetv1()
    else:
        sample_sheet = illumina.SampleSheetv2()

    ############################################################
    # Data section
    ############################################################
    if args.check_duplicate_indexes:
        dupes = sample_sheet.duplicate_indexes()
        if dupes.empty:
            logger.info('No duplicate indexes found.')
        else:
            logger.info('Duplicate indexes found:')
            print(dupes.to_csv(index=False, sep='\t'))
            sys.tracebacklimit = 0
            raise ValueError('Duplicate indexes found in sample sheet.')

    if args.check_hamming_distances:
        # If the allowed "barcode mismatches" is 1, then the minimum hamming distance is 2, etc.
        if args.min_hamming_distance:
            min_hamming_distance = args.min_hamming_distance
        elif args.barcode_mismatches:
            min_hamming_distance = args.barcode_mismatches + 1
        else:
            raise ValueError('You must provide either --min-hamming-distance or --barcode-mismatches.')
        dlist = sample_sheet.hamming_distances()
        insufficient_hamming_distance = False
        for item in dlist:
            # item is a dict with keys: 'u', 'v', 'hamming', and (possibly) 'reverse_complement'
            # u: index1, v: index2, hamming: hamming distance, reverse_complement: 0=>u, 1=>v
            if item['hamming'] < min_hamming_distance:
                insufficient_hamming_distance = True
                # ---
                # [TODO] Add 'filter_indexes' method to SampleSheet class.
                # Takes and `index` and optional `index2` argument and return
                # samples (rows) with matching index.
                # >>> def filter_indexes(self, indexes, which='both', as_mask=False):
                # >>>     # indexes is a list of indexes to check.
                # >>>     if which == 'both':
                # >>>         <check passed indexes against both 'index' and 'index2' in the data>
                # >>>     elif which == 'index1':
                # >>>         <only check against index1>
                # >>>     elif which == 'index2':
                # >>>         <only check against index2>
                # ---
                # match_index1 = df['index'] == str(item['u'])
                # match_index2 = df['index2'] == str(item['v'])
                # print(f'{item["u"]} vs {item["v"]} hamming={item["hamming"]} (rc: {item.get("reverse_complement", None)})')
                # print(df[match_index1 | match_index2])
                # print()
                indexes = [item['u'], item['v']]
                df = sample_sheet.filter_sample_indexes(indexes, which='both', as_mask=False)
                # logger.info(f'{item["u"]} vs {item["v"]} hamming={item["hamming"]} (rc: {item.get("reverse_complement", None)})')
                # logger.info(df.to_csv(index=False, sep='\t'))
        if insufficient_hamming_distance:
            logger.warning('Insufficient Hamming distance found between some indexes. See above for details.')
            sys.exit(1)

    if args.project_suffix:
        if sample_sheet.Data.data.get('Sample_Project') is not None:
            # v1 sample sheets only have the 'Data' section. For v2 sample
            # sheets, 'Data' is aliased to 'BCLConvert_Data'.
            mapper = illumina.get_sample_project(
                sample_sheet.Data.data,
                project_col='Sample_Project'
            )
        elif sample_sheet.format == 'v2' and sample_sheet.Cloud_Data.data.get('ProjectName') is not None:
            # Only v2 sample sheets have 'Cloud_Data'.
            mapper = illumina.get_sample_project(
                sample_sheet.Cloud_Data.data,
                project_col='ProjectName'
            )
        else:
            sys.tracebacklimit = 0
            raise ValueError('Sample sheet does not have a "Sample_Project" or "ProjectName" column.')

        # Add the suffix to the project names.
        mapper = { k: f'{v}_{args.project_suffix}' for k, v in mapper.items() }

        if sample_sheet.Data.data.get('Sample_Project') is not None:
            illumina.set_sample_project(
                sample_sheet.Data.data,
                mapper,
                samples_col='Sample_ID',
                project_col='Sample_Project',
            )

        if sample_sheet.format == 'v2' and sample_sheet.Cloud_Data.data.get('ProjectName') is not None:
            illumina.set_sample_project(
                sample_sheet.Cloud_Data.data,
                mapper,
                samples_col='Sample_ID',
                project_col='ProjectName',
            )

    if args.projectname_to_sampleproject:
        if sample_sheet.format == 'v1':
            sys.tracebacklimit = 0
            raise ValueError('Project name to sample project mapping is only valid for v2 sample sheets.')
        # Only valid for V2 sample sheets.
        sample_sheet.projectname_to_sampleproject()

    if args.merge_duplicate_indexes:
        sample_sheet.merge_duplicate_indexes()
    
    ############################################################
    # Settings section
    ############################################################
    if args.create_fastq_for_index_reads:
        if sample_sheet.format == 'v1':
            sample_sheet.Settings.CreateFastqForIndexReads = 1
        elif sample_sheet.format == 'v2':
            sample_sheet.BCLConvert_Settings.CreateFastqForIndexReads = 1

    ############################################################
    # Print to stdout
    ############################################################
    print(sample_sheet.to_csv())


def run_list(args):
    '''
    List info for sequencing runs or sample sheets. Possible inputs:

    - A directory containing sequencing runs.
    - A sequencing run directory.
    - A sample sheet.
    '''
    sample_sheets = []
    run_dirs = []
    info = []
    for path in args.pathlist:
        if not os.path.exists(path):
            sys.tracebacklimit = 0
            raise FileNotFoundError(f'Path does not exist: {path}')
        if illumina.looks_like_samplesheet(path):
            logger.debug(f'Found sample sheet: {path}')
            sample_sheets.append(path)
        elif os.path.isdir(path):
            if illumina.is_runid(os.path.basename(path)):
                logger.debug(f'Found runid: {path}')
                run_dirs.append(path)
            else:
                for rundir in reports.find_seq_runs(path):
                    run_dirs.append(rundir)

    for path in sample_sheets:
        try:
            sample_sheet = illumina.read_sample_sheet(path)
            info.append(sample_sheet.info)
        except Exception as e:
            logger.debug(f'Error reading {path}: {e}')
            continue

    for path in run_dirs:
        try:
            seqrun = illumina.IlluminaSequencingData(path)
        except Exception as e:
            logger.debug(f'Error reading {path}: {e}')
            continue
        if not seqrun.path_to_samplesheet.exists():
            sample_sheets = reports.find_samplesheets(path)
            if len(sample_sheets) == 1:
                seqrun.path_to_samplesheet = sample_sheets[0]
            elif len(sample_sheets) > 1:
                logger.debug(f'Multiple sample sheets found in {path}')
                continue
        info.append(seqrun.info)
    # The 'table' is a string.
    table = reports.list_runs(
        info,
        long=args.long,
        transpose=args.transpose,
        sep=args.sep
    )
    print(table)
    return 0


def run_bcl2fastq(args):
    logger.debug(f'GENSVC_PROCDATA={config.GENSVC_PROCDATA}')
    seqrun = sequencing_run.IlluminaSequencingData(args.runfolder_dir)
    # print(seqrun.info)
    # seqrun.init_procdir()

    # Do not use os.cpu_count() with sbatch.
    # TODO Have the batch script get the number of cpus from the slurm environment.
    command = bcl2fastq.bcl2fastq(
        runfolder_dir=seqrun.realpath,
        sample_sheet=args.sample_sheet or seqrun.path_to_samplesheet,
        output_dir=args.output_dir or bcl2fastq.init_output_dir(config.GENSVC_PROCDATA, seqrun.runid),
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
    logger.debug(procdata)

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

    # ************************************************************
    # Subparsers
    # ************************************************************
    subparsers = parser.add_subparsers(help='sub-command help')

    # ************************************************************
    # Set up a new sample sheet.
    # ************************************************************
    parse_sample_sheet = subparsers.add_parser(
        'samplesheet',
        help='Generate a sample sheet for BCL-Convert.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_sample_sheet.set_defaults(func=cli_sample_sheet)
    parse_sample_sheet.add_argument(
        '--format', '-F',
        choices=['v1', 'v2'],
        default='v2',
        help='Sample sheet format.'
    )
    parse_sample_sheet.add_argument(
        '--from', '-f',
        dest='src_sample_sheet',  # `from` is a reserved word.
        action='store',
        type=pathlib.Path,
        help='Initialize new sample sheet from the given sample sheet.'
    )
    parse_sample_sheet.add_argument(
        '--projectname2sampleproject', '-p',
        dest='projectname_to_sampleproject',
        action='store_true',
        default=False,
        help='Map the "ProjectName" from "Cloud_Data" to "Sample_Project" in "BCLConvert_Data".'
    )
    parse_sample_sheet.add_argument(
        '--project-suffix', '-s',
        action='store',
        type=str,
        help=''
    )
    parse_sample_sheet.add_argument(
        '--check-duplicate-indexes', '-i',
        action='store_true',
        default=False,
        help='Check for duplicate indexes in the sample sheet.'
    )
    parse_sample_sheet.add_argument(
        '--merge-duplicate-indexes', '-m',
        action='store_true',
        default=False,
        help='Merge samples with duplicate indexes into a single dummy sample.'
    )
    parse_sample_sheet.add_argument(
        '--check-hamming-distances',
        action='store_true',
        default=False,
        help='Check pairwise Hamming distances between sample indexes. Requires either --barcode-mismatches or --min-hamming-distance to be set.'
    )
    parse_sample_sheet.add_argument(
        '--barcode-mismatches',
        action='store',
        type=int,
        default=None,
        help='Number of allowed barcode (index) mismatches.'
    )
    parse_sample_sheet.add_argument(
        '--min-hamming-distance',
        action='store',
        type=int,
        default=None,
        help=(
            'Minimum Hamming distance between sample barcodes (indexes). '
            'If not provided, is set to --barcode-mismatches+1. '
            'If provided, --barcode-mismatches will be ignored.'
        )
    )
    parse_sample_sheet.add_argument(
        '--create-fastq-for-index-reads',
        action='store_true',
        default=False,
        help='Create FASTQ files for index reads.'
    )

    # ************************************************************
    # Generate summary stats.
    # ************************************************************
    parse_extract_bcl2fastq_stats = subparsers.add_parser(
        'extract-bcl2fastq-stats', 
        aliases=['ex'],
        help=bcl2fastq.__doc__.strip().split('\n')[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_extract_bcl2fastq_stats.set_defaults(func=extract_bcl2fastq_stats)
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

    # ************************************************************
    # List sequencing runs.
    # ************************************************************
    parse_reports = subparsers.add_parser(
        'list', 
        aliases=['ls', 'scan', 'sc'],
        help='List sequencing runs in the given director',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_reports.set_defaults(func=run_list)
    parse_reports.add_argument(
        'pathlist',  
        default=[
            config.GENSVC_ISEQ_DATADIR,
            config.GENSVC_NEXTSEQ_DATADIR,
            config.GENSVC_NOVASEQ_DATADIR
        ],
        nargs='*',
        help='The path(s) to the directory containing the sequencing runs.'
    )
    parse_reports.add_argument(
        '-l', '--long',
        action='store_true',
        help='List more stuff.'
    )
    parse_reports.add_argument(
        '-s', '--sep',
        action='store',
        default='\t',
        type=str,
        help='Column separator.'
    )
    parse_reports.add_argument(
        '-t', '--transpose',
        action='store_true',
        help='Transpose the table.'
    )

    # ************************************************************
    # Convert BCL files to FASTQ.
    # ************************************************************
    parse_converter = subparsers.add_parser(
        'convert', 
        aliases=['co'],
        help='Convert BCL files to FASTQ using Illumina\'s bcl2fastq.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_converter.set_defaults(func=run_bcl2fastq)
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

    # ************************************************************
    # Trasfer data to user's project directory.
    # ************************************************************
    parse_setup_transfer = subparsers.add_parser(
        'setup_transfer', 
        aliases=['se'],
        help='Set up data for tranfer.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_setup_transfer.set_defaults(func=run_setup_transfer)
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

    # ************************************************************
    # Trasfer data to user's project directory.
    # ************************************************************
    parse_transfer = subparsers.add_parser(
        'transfer', 
        aliases=['tr'],
        help='Transfer results to project directory.'
    )
    parse_transfer.set_defaults(func=run_transfer)
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

    return parser

def main():

    args = get_parser().parse_args()

    # Initialize logging.
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    # Set up logging.

    logger.info('*** Logger info is working ***')
    logger.debug('*** Logger debug is working ***')

    res = args.func(args)

    return res

#!/usr/bin/env python
# coding: utf-8

'''
Helper for working with data from the Genomics Core.
'''

import argparse
import logging
import sys

from gensvc.core_facility import archive
from gensvc.core_facility import trash
from gensvc.misc.config import config

from pathlib import Path

logger = logging.getLogger(__name__)


def cli_create_samplesheet(args):
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


def cli_list(args):
    from gensvc.core_facility.reports import cli_list
    res = cli_list(args)
    return res


def cli_list_run(args):
    from gensvc.core_facility.sequencing_run import cli
    return cli(args)


def run_transfer(args):
    pass


def cli_bclconvert(args):
    from gensvc.wrappers.bclconvert import cli
    return cli(args)


def cli_extract_bclconvert_stats(args):
    from gensvc.core_facility import postprocess
    _ = postprocess.cli_extract_bclconvert_stats(args)
    return 0


def cli_setup_transfer_dirs(args):
    from gensvc.core_facility import transfer
    _ = transfer.cli_setup_transfer_dirs(args)
    return 0


def cli_config(args):
    from gensvc.misc.config import cli
    return cli(args)


def cli_samplesheet(args):
    from gensvc.cli.samplesheet import cli
    return cli(args)


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
        action='count',
        default=0,
        help='Increase verbosity level (can be used multiple times). If once, set logging level to INFO, if twice or more, set to DEBUG.'
    )

    # ============================================================
    # Initialize Subparsers
    # ============================================================
    subparsers = parser.add_subparsers(help='sub-command help')

    parse_config = subparsers.add_parser(
        'config',
        help='View or set configuration settings.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_config.set_defaults(func=cli_config)

    parse_reports = subparsers.add_parser(
        'list', 
        aliases=['ls'],
        help='List sequencing runs in the given director',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_reports.set_defaults(func=cli_list)

    parse_list_run = subparsers.add_parser(
        'list-run', 
        help='List sequencing run by run ID in the given director',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_list_run.set_defaults(func=cli_list_run)

    parse_create_samplesheet = subparsers.add_parser(
        'create-samplesheet',
        help='Create a sample sheet for BCL Convert.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_create_samplesheet.set_defaults(func=cli_create_samplesheet)

    # gensvc samplesheet [create|update]
    parse_samplesheet = subparsers.add_parser(
        'samplesheet',
        help='Create a sample sheet for BCL Convert.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    subparse_samplesheet = parse_samplesheet.add_subparsers(help='samplesheet sub-command help')

    # gensvc samplesheet update
    parse_samplesheet_update = subparse_samplesheet.add_parser(
        'update',
        help='Update an existing sample sheet.'
    )
    parse_samplesheet_update.set_defaults(
        func=cli_samplesheet,
        subcommand='update'
    )

    # gensvc samplesheet create
    parse_samplesheet_create = subparse_samplesheet.add_parser(
        'create',
        help='Create an new sample sheet.'
    )
    parse_samplesheet_create.set_defaults(
        func=cli_samplesheet,
        subcommand='create'
    )

    # gensvc bclconvert <runid_or_rundir>
    parser_bclconvert = subparsers.add_parser(
        'bclconvert', 
        help='Convert BCL files to FASTQ using Illumina\'s BCLConvert.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser_bclconvert.set_defaults(func=cli_bclconvert)

    # ============================================================
    # Config
    # ============================================================
    parse_config.add_argument(
        '--list', '-l',
        help='List current configuration settings.',
        action='store_true',
        default=False,
    )

    # ============================================================
    # Sample Sheets
    # ============================================================
    parse_samplesheet_update.add_argument(
        'path_or_runid',
        help='Can be a path to a sample sheet (file), path to a sequencing run (directory), or run ID.',
        action='store',
    )
    for p in [parse_samplesheet_create, parse_samplesheet_update]:
        p.add_argument(
            '--output', '-o',
            help='Path to output sample sheet file. If "-", print to stdout.',
            action='store',
            default=None
        )
        p.add_argument(
            '--force',
            help='Overwrite existing sample sheet file.',
            action='store_true',
            default=False,
        )
        p.add_argument(
            '--from', '-f',
            help='Initialize sample sheet from the given sample sheet.',
            dest='src_sample_sheet',  # `from` is a reserved word.
            action='store',
            type=Path,
        )
        p.add_argument(
            '--format', '-F',
            choices=['v1', 'v2'],
            default='v2',
            help='Sample sheet format.'
        )
        p.add_argument(
            '--fix-sample-names',
            help='Fix sample names to be compatible with Illumina requirements.',
            action='store_true',
            default=True,
        )
        p.add_argument(
            '--no-fix-sample-names',
            dest='fix_sample_names',
            action='store_false',
            help='Do not fix sample names.'
        )
        p.add_argument(
            '--projectname-to-sampleproject', '-p',
            dest='projectname_to_sampleproject',
            action='store_true',
            default=False,
            help='Map the "ProjectName" from "Cloud_Data" to "Sample_Project" in "BCLConvert_Data".'
        )
        p.add_argument(
            '--project-suffix', '-s',
            action='store',
            type=str,
            help=''
        )
        p.add_argument(
            '--check-duplicate-indexes', '-i',
            action='store_true',
            default=False,
            help='Check for duplicate indexes in the sample sheet.'
        )
        p.add_argument(
            '--merge-duplicate-indexes', '-m',
            action='store_true',
            default=False,
            help='Merge samples with duplicate indexes into a single dummy sample.'
        )
        p.add_argument(
            '--check-hamming-distances',
            action='store_true',
            default=False,
            help='Check pairwise Hamming distances between sample indexes. Requires either --barcode-mismatches or --min-hamming-distance to be set.'
        )
        p.add_argument(
            '--barcode-mismatches',
            action='store',
            type=int,
            default=None,
            help='Number of allowed barcode (index) mismatches.'
        )
        p.add_argument(
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
        p.add_argument(
            '--create-fastq-for-index-reads',
            action='store_true',
            default=False,
            help='Create FASTQ files for index reads.'
        )

    # ============================================================
    # DEPRECATED - Set up a new sample sheet.
    # Use `gensvc samplesheet create` instead.
    # ============================================================
    parse_create_samplesheet.add_argument(
        '--format', '-F',
        choices=['v1', 'v2'],
        default='v2',
        help='Sample sheet format.'
    )
    parse_create_samplesheet.add_argument(
        '--from', '-f',
        dest='src_sample_sheet',  # `from` is a reserved word.
        action='store',
        type=Path,
        help='Initialize new sample sheet from the given sample sheet.'
    )
    parse_create_samplesheet.add_argument(
        '--projectname-to-sampleproject', '-p',
        dest='projectname_to_sampleproject',
        action='store_true',
        default=False,
        help='Map the "ProjectName" from "Cloud_Data" to "Sample_Project" in "BCLConvert_Data".'
    )
    parse_create_samplesheet.add_argument(
        '--project-suffix', '-s',
        action='store',
        type=str,
        help=''
    )
    parse_create_samplesheet.add_argument(
        '--check-duplicate-indexes', '-i',
        action='store_true',
        default=False,
        help='Check for duplicate indexes in the sample sheet.'
    )
    parse_create_samplesheet.add_argument(
        '--merge-duplicate-indexes', '-m',
        action='store_true',
        default=False,
        help='Merge samples with duplicate indexes into a single dummy sample.'
    )
    parse_create_samplesheet.add_argument(
        '--check-hamming-distances',
        action='store_true',
        default=False,
        help='Check pairwise Hamming distances between sample indexes. Requires either --barcode-mismatches or --min-hamming-distance to be set.'
    )
    parse_create_samplesheet.add_argument(
        '--barcode-mismatches',
        action='store',
        type=int,
        default=None,
        help='Number of allowed barcode (index) mismatches.'
    )
    parse_create_samplesheet.add_argument(
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
    parse_create_samplesheet.add_argument(
        '--create-fastq-for-index-reads',
        action='store_true',
        default=False,
        help='Create FASTQ files for index reads.'
    )

    # ============================================================
    # Generate summary stats from bclconvert output.
    # ============================================================
    parse_extract_bclconvert_stats = subparsers.add_parser(
        'extract-bclconvert-stats', 
        help='Extract summary statistics from bclconvert output.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_extract_bclconvert_stats.set_defaults(func=cli_extract_bclconvert_stats)
    parse_extract_bclconvert_stats.add_argument(
        'bclconvert_directory',  # Must be underscore.
        action='store',
        type=Path,
        help='The BCLConvert output directory containing the "Reports" and "Stats" subdirectories.'
    )
    parse_extract_bclconvert_stats.add_argument(
        '--outdir',
        action='store',
        default=None,
        type=Path,
        help='Directory in which to put the output files. If not given, will be placed next to the BCLConvert output directory and named "SummaryStatistics".'
    )

    # ============================================================
    # List sequencing runs.
    # ------------------------------------------------------------
    # The reports.cli_list fxn (the default for the parse_reports parser) is
    # deprecated. Use sequencing_run.cli (parse_list_run) instead.
    # ============================================================
    parse_list_run.add_argument(
        'runid',
        help='The run ID or path to sequencing run.'
    )

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

    # ============================================================
    # Convert BCL files to FASTQ.
    # ============================================================
    parser_bclconvert.add_argument(
        'runid_or_rundir',
        help='The run ID or path to sequencing run directory.',
        action='store',
        default=None
    )
    parser_bclconvert.add_argument(
        '--path-to-bclconvert-exe',
        help='The path to the BCLConvert executable.',
        action='store',
        type=Path,
        default=config.PATH_TO_BCLCONVERT_EXE
    )
    parser_bclconvert.add_argument(
        '--bcl-input-directory',
        help='The path to the sequencing run directory.',
        action='store',
        type=Path,
        default=None
    )
    parser_bclconvert.add_argument(
        '--output-directory',
        help='The path to the BCLConvert output directory.',
        action='store',
        type=Path,
        default=None
    )
    parser_bclconvert.add_argument(
        '--sample-sheet',
        help='The path to the SampleSheet.csv file to use.',
        action='store',
        type=Path,
        default=None
    )
    parser_bclconvert.add_argument(
        '--sample-name-column-enabled',
        help='Use the Sample_Name column when generating FASTQ file names.',
        action='store_true',
        default=True
    )
    parser_bclconvert.add_argument(
        '--bcl-sampleproject-subdirectories',
        help='Group output FASTQ files into subdirectories for each Sample_Project under the output directory.',
        action='store_true',
        default=True
    )
    parser_bclconvert.add_argument(
        '--output-legacy-stats',
        help='Also generate legacy stats files (similar to bcl2fastq output).',
        action='store_true',
        default=True
    )
    parser_bclconvert.add_argument(
        '--dump',
        help='Print the batch script to stdout.',
        action='store_true',
        default=False
    )
    parser_bclconvert.add_argument(
        '--run',
        help='Run the BCLConvert conversion immediately in the current compute environment.',
        action='store_true',
        default=False
    )
    parser_bclconvert.add_argument(
        '--sbatch',
        help='Use sbatch to submit the BCLConvert conversion as a Slurm job.',
        action='store_true',
        default=False
    )
    parser_bclconvert.add_argument(
        '--srun',
        help='Use srun to run BCLConvert in a Slurm job step (or request a new job).',
        action='store_true',
        default=False
    )

    # ============================================================
    # Set up transfer directories and scripts.
    # ============================================================
    parser_setup_transfer_dirs = subparsers.add_parser(
        'split-shared-run', 
        help='Set up transfer directories and corresponding scripts for each "sample project".',
        aliases=['setup-transfer'],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser_setup_transfer_dirs.set_defaults(func=cli_setup_transfer_dirs)
    parser_setup_transfer_dirs.add_argument(
        'procdir',  
        action='store',
        type=Path,
        help='The path to the processed data directory.'
    )
    parser_setup_transfer_dirs.add_argument(
        '-s', '--sbatch',  
        action='store_true',
        help='Run transfer as a Slurm job.'
    )

    # ============================================================
    # Archive
    # ============================================================
    parse_archive = subparsers.add_parser(
        'archive',
        help='Create tar archives of sequencing runs.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_archive.set_defaults(func=archive.cli)

    parse_archive.add_argument(
        '--overwrite',
        action='store_true',
        default=False,
        help='Overwrite existing job scripts.'
    )

    # ============================================================
    # Trash
    # ============================================================
    parse_trash = subparsers.add_parser(
        'trash',
        help='Create a script for deleting sequencing runs older than six months.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parse_trash.set_defaults(func=trash.cli)

    parse_trash.add_argument(
        '--output', '-o',
        action='store',
        type=Path,
        default=None,
        help='Path to output script file. If not provided, script is printed to stdout.'
    )

    return parser.parse_args()


def main():

    args = get_parser()
    # print(args)

    # Initialize logger.
    if args.verbose >= 2:
        logger.setLevel(logging.DEBUG)
    elif args.verbose == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    # # Test the logger.
    # logger.warning('*** Logger WARNING is working ***')
    # logger.info('*** Logger INFO is working ***')
    # logger.debug('*** Logger DEBUG is working ***')

    if args.verbose >= 2:
        logger.debug(f'Arguments: {args}')

    res = args.func(args)

    return res

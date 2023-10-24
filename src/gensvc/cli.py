#!/usr/bin/env python
# coding: utf-8

'''
Helper for working with data from the Genomics Core.
'''

# Python standard library
import pathlib
import argparse
import sys

from gensvc.misc import bcl2fastq, reports, slurm, sequencing_run


def run_scan(args):
    reports.scan_dir(args.directory)
    return 0


def run_bcl2fastq(args):
    seqrun = sequencing_run.SeqRun(
        rundir=args.rundir,
        procdir=args.outdir
    )
    print(seqrun.info)
    # seqrun.init_procdir()
    command = bcl2fastq.bcl2fastq(
        seqrun=seqrun
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


def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
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
        help=bcl2fastq.__doc__.strip().split('\n')[0]
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

    # Scan for sequencing runs.
    parse_reports = subparsers.add_parser(
        'scan', 
        aliases=['sc'],
        help='Scan the given directory for sequencing runs'
    )

    parse_reports.add_argument(
        'directory',  # Must be underscore.
        action='store',
        type=pathlib.Path,
        help='The parent directory to scan.'
    )

    parse_reports.set_defaults(func=run_scan)

    # Convert BCL files to FASTQ.
    parse_converter = subparsers.add_parser(
        'convert', 
        aliases=['co'],
        help='Convert BCL files to FASTQ using Illumina\'s bcl2fastq.'
    )

    parse_converter.add_argument(
        'rundir',  # Must be underscore.
        action='store',
        type=pathlib.Path,
        help='Path to sequencing run.'
    )

    parse_converter.add_argument(
        '-o', '--outdir',  # Must be underscore.
        action='store',
        type=pathlib.Path,
        help='Path to output directory.'
    )

    parse_converter.add_argument(
        '-s', '--sbatch',  # Must be underscore.
        action='store_true',
        help='Submit bcl2fastq job to Slurm using `sbatch`.'
    )

    parse_converter.set_defaults(func=run_bcl2fastq)

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

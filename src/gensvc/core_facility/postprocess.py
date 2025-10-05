#!/usr/bin/env python
# coding: utf-8

'''
Step 1: Extract sequencing statistics from bcl2fastq outputs or from
BCL-Convert legacy stats.

Briefly, this script extracts tables from HTML files and splits those tables by
project. Tables for split lane runs will be saved in separate directories
according to the "Project" name.

More info: bcl2fastq generates summary stats automatically. The 'raw' stats are
in the Stats directory (as json) and the summary tables for users are in the
Reports directory (html). This script scrapes most of the summary stats from
the html files in the Reports directory. The only exception is the "Top Unknown
Barcodes" table; the formatting in the html files is awkward, so we recreate
this one from json in the Stats directory.

The output of this script is organized as follows:

    SummaryStatistics
    ├── <PROJECT>
    │   ├── LaneSummary.csv
    │   ├── SampleSummary.csv
    │   └── TopUnknownBarcodes.csv
    └── all
        ├── FlowcellSummary.csv
        ├── LaneSummary.csv
        ├── SampleSummary.csv
        └── TopUnknownBarcodes.csv

Step 2: Set up the transfer directory.

The original data directory has the following structure:

    $PROJECTS/UTK0192/processed/<run_id>/BCLConvert/<sample_project>

And we need to set up the transfer directory that looks like this:

    $PROJECTS/UTK0192/processed/<project>/<run_id>/<sample_project>/fastq
'''

# Python standard library
import argparse
import json
import logging
import os
import pathlib
import re
import shutil
import sys
import warnings

# Third party
import pandas as pd


logger = logging.getLogger(__name__)


def write_table(df, path=None, index=False, dry_run=False):
    if path is None:
        print(df.to_string(index=index))
        return None
    elif dry_run:
        logger.info(f'DRY RUN -> Would save table data to {path}')
        return None
    else:
        logger.debug(f'Saving table data to {path}')
        return df.to_csv(path, index=index)


def write_bclconvert_tables_from_legacy_stats(tables, outdir, dry_run=False):

    # All Lanes: These files are the ones that the Genomics Core will want to
    # look at. They contain summary stats for all sequences in all lanes.
    # outdir.mkdir(parents=True, exist_ok=True)
    out_all = outdir / 'all'
    if not dry_run:
        out_all.mkdir(parents=True, exist_ok=True)

    write_table(
        tables['main_flowcell_summary'],
        path=out_all / 'FlowcellSummary.csv',
        dry_run=dry_run
    )
    write_table(
        tables['main_lane_summary'],
        path=out_all / 'LaneSummary.csv',
        dry_run=dry_run
    )
    write_table(
        tables['samples_lane_summary'],
        path=out_all / 'SampleSummary.csv',
        dry_run=dry_run
    )
    write_table(
        tables['stats_top_unknown_barcodes'],
        path=out_all / 'TopUnknownBarcodes.csv',
        dry_run=dry_run
    )

    # Split Lanes: Each user's data is wholly contained in a subset of the
    # lanes. We only want to provide the summary stats relevant for the lanes
    # that contain that user's data.
    for name, grp in tables['samples_lane_summary'].groupby('Project'):
        if name == 'default':
            continue
        lanes = set(grp['Lane'])
        if outdir:
            out_project = outdir / name
            if not dry_run:
                out_project.mkdir(exist_ok=True)
        else:
            out_project = None

        # Sample Summary
        write_table(
            grp,
            path=out_project / 'SampleSummary.csv',
            dry_run=dry_run
        )

        # Lane Summary
        mask = tables['main_lane_summary']['Lane'].isin(lanes)
        write_table(
            tables['main_lane_summary'].loc[mask],
            path=out_project / 'LaneSummary.csv',
            dry_run=dry_run
        )

        # Top Unknown Barcodes
        mask = tables['stats_top_unknown_barcodes']['Lane'].isin(lanes)
        write_table(
            tables['stats_top_unknown_barcodes'].loc[mask],
            path=out_project / 'TopUnknownBarcodes.csv',
            dry_run=dry_run
        )

    return None


def cli_extract_bclconvert_stats(args):
    from gensvc.wrappers import bclconvert

    # Initialize logger.
    if args.verbose >= 2:
        logger.setLevel(logging.DEBUG)
    elif args.verbose == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)
    
    # Check for output directory.
    if args.outdir is None:
        outdir = args.bclconvert_directory.parent / 'SummaryStatistics'
    else:
        outdir = args.outdir

    # Extract tables from legacy stats.
    tables = bclconvert.extract_tables_from_legacy_stats(args.bclconvert_directory)
    write_bclconvert_tables_from_legacy_stats(tables, outdir, dry_run=args.dry_run)

    # Create the "Suggested Barcodes" report.
    bclconvert_reports = bclconvert.BCLConvertReports(args.bclconvert_directory / 'Reports')

    demultiplex_stats = bclconvert_reports.load_demultiplex_stats()
    top_unknown_barcodes = bclconvert_reports.load_top_unknown_barcodes()

    suggested_barcodes = bclconvert.report_suggested_barcodes(
        demultiplex_stats,
        top_unknown_barcodes
    )

    if suggested_barcodes.empty:
        logger.warning('No suggested barcodes found!')
        # [TODO] The 'suggested_barcodes' function should return
        # ('Sample_Project', 'NA') if there are no suggested barcodes instead
        # of being empty.
    else:
        write_table(
            suggested_barcodes,
            path=outdir / 'all/SuggestedBarcodes.csv',
            dry_run=args.dry_run
        )

    return 0


# END

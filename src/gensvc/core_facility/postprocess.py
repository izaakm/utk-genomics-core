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


def write_table(df, path=None, index=False):
    if path is None:
        print(df.to_string(index=index))
        return None
    else:
        logging.debug(f'Saving table data to {path}')
        return df.to_csv(path, index=index)


def write_bclconvert_tables_from_legacy_stats(tables, outdir):

    # All Lanes: These files are the ones that the Genomics Core will want to
    # look at. They contain summary stats for all sequences in all lanes.
    # outdir.mkdir(parents=True, exist_ok=True)
    out_all = outdir / 'all'
    out_all.mkdir(parents=True, exist_ok=True)

    write_table(
        tables['main_flowcell_summary'],
        path=out_all / 'FlowcellSummary.csv'
    )
    write_table(
        tables['main_lane_summary'],
        path=out_all / 'LaneSummary.csv'
    )
    write_table(
        tables['samples_lane_summary'],
        path=out_all / 'SampleSummary.csv'
    )
    write_table(
        tables['stats_top_unknown_barcodes'],
        path=out_all / 'TopUnknownBarcodes.csv'
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
            out_project.mkdir(exist_ok=True)
        else:
            out_project = None

        # Sample Summary
        write_table(
            grp,
            path=out_project / 'SampleSummary.csv'
        )

        # Lane Summary
        mask = tables['main_lane_summary']['Lane'].isin(lanes)
        write_table(
            tables['main_lane_summary'].loc[mask],
            path=out_project / 'LaneSummary.csv'
        )

        # Top Unknown Barcodes
        mask = tables['stats_top_unknown_barcodes']['Lane'].isin(lanes)
        write_table(
            tables['stats_top_unknown_barcodes'].loc[mask],
            path=out_project / 'TopUnknownBarcodes.csv'
        )

    return None


def cli_extract_bclconvert_stats(args):
    from gensvc.wrappers import bclconvert
    if args.outdir is None:
        outdir = args.bclconvert_directory.parent / 'SummaryStatistics'
    else:
        outdir = args.outdir
    tables = bclconvert.extract_tables_from_legacy_stats(args.bclconvert_directory)
    write_bclconvert_tables_from_legacy_stats(tables, outdir)
    return 0

# END

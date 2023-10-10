#!/usr/bin/env python
# coding: utf-8

'''
Extract sequencing statistics from bcl2fastq outputs.

Briefly, this script parses stats from bcl2fastq and splits them by project.
Tables for split lane runs will be saved in separate directories according to
the "Project" name.

More info: bcl2fastq generates summary stats automatically. The 'raw' stats are
in the Stats directory (as json) and the summary tables for users are in the
Reports directory (html). This script scrapes most of the summary stats from
the html files in the Reports directory. The only exception is the "Top Unknown
Barcodes" table; the formatting in the html files is awkward, so we recreate
this one from json in the Stats directory.

The output of this script is organized as follows:

    summary-stats
    ├── <PROJECT>
    │   ├── LaneSummary.csv
    │   ├── SampleSummary.csv
    │   └── TopUnknownBarcodes.csv
    └── all
        ├── FlowcellSummary.csv
        ├── LaneSummary.csv
        ├── SampleSummary.csv
        └── TopUnknownBarcodes.csv
'''

# Python standard library
import pathlib
import json
import argparse
import sys

# Third party
import pandas as pd
from sample_sheet import SampleSheet
# import multiqc


def get_sample_stats(sd, lc):
    '''
    sd : Sample Demultiplex Results
    lc : Lane Conversion Results
    '''

    barcode_sequence = sd['IndexMetrics'][0]['IndexSequence']
    sample_nreads = sd['NumberReads']
    if sample_nreads > 0:
        lane_nclusters = lc['TotalClustersPF']
        percent_lane = (sample_nreads / lane_nclusters)*100
        percent_perfect_barcode = sd['IndexMetrics'][0]['MismatchCounts']['0'] / sd['NumberReads'] * 100
        percent_mismatch_barcode = sd['IndexMetrics'][0]['MismatchCounts']['1'] / sd['NumberReads'] * 100
        yield_mbases = round(sd['Yield'] / 1000000)

        # Percent bases w/ Q30
        y0 = sd['ReadMetrics'][0]['YieldQ30'] / sd['ReadMetrics'][0]['Yield']
        y1 = sd['ReadMetrics'][1]['YieldQ30'] / sd['ReadMetrics'][1]['Yield']
        percent_q30 = ((y0+y1)/2)*100
        
        # Mean Quality score
        q0 = sd['ReadMetrics'][0]['QualityScoreSum'] / sd['ReadMetrics'][0]['Yield']
        q1 = sd['ReadMetrics'][1]['QualityScoreSum'] / sd['ReadMetrics'][1]['Yield']
        mean_quality = (q0+q1)/2

    else:
        lane_nclusters = 0
        percent_lane = 'NA'
        percent_perfect_barcode = 'NA'
        percent_mismatch_barcode = 'NA'
        yield_mbases = 'NA'
        percent_q30 = 'NA'
        mean_quality = 'NA'
    
    row = {
        'Project': '',
        'Sample': sd['SampleName'],
        'Barcode sequence': barcode_sequence,
        'PF Clusters': sample_nreads,
        '% of the lane': percent_lane,
        '% Perfect barcode': percent_perfect_barcode,
        '% One mismatch barcode': percent_mismatch_barcode,
        'Yield (Mbases)': yield_mbases,
        # '% PF Clusters': 'Always 100???',
        '% >= Q30 bases': percent_q30,
        'Mean Quality Score': mean_quality
    }
    return row


def make_samples_lane_summary(statsdata):
    demux_data = []
    for lane_number, lane in enumerate(statsdata['ConversionResults'], start=1):
        # print(lane_number)
        for sample in lane['DemuxResults']:
            # print(sample['SampleName'])
            demux_data.append(get_sample_stats(sd=sample, lc=lane))
    demux_df = pd.DataFrame(demux_data)
    return demux_df


def get_top_unknown_barcodes(statsdata, n=10):
    top_ubarcodes = []
    for lane in statsdata['UnknownBarcodes']:
        # print(lane['Lane'])
        # print(lane.keys())
    
        b = lane['Barcodes']
        
        t = pd.Series(b)
        t.name = 'Count' # Will be coerced to column name.
        t = t.to_frame()
        t = t.reset_index(names=['Sequence'])
        t['Lane'] = lane['Lane']
        top_ubarcodes.append(t.sort_values('Count', ascending=False).head(10).copy())
    table = pd.concat(top_ubarcodes).reset_index(drop=True)
    table = table[['Lane', 'Count', 'Sequence']]
    return table


def extract_tables(fastqdir):
    tables = {}
    p = list(fastqdir.rglob('all/all/all/lane.html'))[0]
    data = pd.read_html(p)
    # len(data)

    # data[0]
    tables['main_flowcell_summary'] = data[1]
    tables['main_lane_summary'] = data[2]
    
    p = list(fastqdir.rglob('all/all/all/laneBarcode.html'))[0]
    data = pd.read_html(p)
    # len(data)
    
    # data[0]
    tables['samples_flowcell_summary'] = data[1]
    tables['samples_lane_summary'] = data[2]
    tables['samples_top_unknown_barcodes'] = data[3]

    return tables

def write_table(df, path, index=False, dry_run=False):
    if dry_run:
        print(df.to_string(index=index))
        return None
    else:
        return df.to_csv(path, index=index)

def main():

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'fastqdir',
        action='store',
        type=pathlib.Path,
        help='The bcl2fastq output directory containing the "Reports" and "Stats" subdirectories.'
    )

    parser.add_argument(
        '--statsfile',
        action='store',
        type=pathlib.Path,
        help=(
            'Optional: Statistics json file generated by bcl2fastq, eg "<bcl2fastq output>/Stats/Stats.json". '
            'If --fastqdir is provided, this defaults to "<fastqdir>/Stats/Stats.json".'
        )
    )

    parser.add_argument(
        '--outdir',
        action='store',
        default='SummaryStatistics',
        type=pathlib.Path,
        help='Directory in which to put the output files.'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=False,
        help='Just print the output.'
    )

    # parser.add_argument(
    #     '--samplesheetfile',
    #     action='store',
    #     type=pathlib.Path,
    #     help='Sample sheet used as input for sequencing and bcl2fastq.'
    # )

    args = parser.parse_args()
    # print(args)

    # Scrape tables from html files in the Reports directory.
    tables = extract_tables(args.fastqdir)

    # Load stats data.
    if args.statsfile:
        with open(args.statsfile) as f:
            statsdata = json.load(f)
    else:
        with open(args.fastqdir / 'Stats/Stats.json') as f:
            statsdata = json.load(f)

    tables['stats_lane_summary'] = make_samples_lane_summary(statsdata)

    # # Load the sample sheet.
    # ss = SampleSheet(args.samplesheetfile)
    # sample_df = pd.DataFrame([sample.to_json() for sample in ss.samples])
    # 
    # # Merge stats with sample sheet.
    # foo = pd.merge(
    #     left=sample_df,
    #     right=demux_df,
    #     left_on='Sample_Name',
    #     right_on='Sample'
    # )
    # # [TODO] Match columns from report.
    # for name, grp in foo.groupby('Sample_Project'):
    #     grp.to_csv(
    #         f'SequencingStatisticsSampleSummary-{name}.csv',
    #         index=False
    #     )

    # Get unknown barcodes
    tables['stats_top_unknown_barcodes'] = get_top_unknown_barcodes(statsdata, n=10)

    # Debugging.
    # print(tables)
    # for k in tables:
    #     t = tables[k]
    #     print(k)
    #     print(t)
    
    # All Lanes: These files are the ones that the Genomics Core will want to
    # look at. They contain summary stats for all sequences in all lanes.
    # args.outdir.mkdir(parents=True, exist_ok=True)
    out_all = args.outdir / 'all'
    out_all.mkdir(parents=True, exist_ok=True)
    _ = write_table(tables['main_flowcell_summary'], out_all / 'FlowcellSummary.csv', dry_run=args.dry_run)
    _ = write_table(tables['main_lane_summary'], out_all / 'LaneSummary.csv', dry_run=args.dry_run)
    _ = write_table(tables['samples_lane_summary'], out_all / 'SampleSummary.csv', dry_run=args.dry_run)
    _ = write_table(tables['stats_top_unknown_barcodes'], out_all / 'TopUnknownBarcodes.csv', dry_run=args.dry_run)

    # Split Lanes: Each user's data is wholly contained in a subset of the
    # lanes. We only want to provide the summary stats relevant for the lanes
    # that contain that user's data.
    for name, grp in tables['samples_lane_summary'].groupby('Project'):
        if name == 'default':
            continue
        lanes = set(grp['Lane'])
        out_project = args.outdir / name
        out_project.mkdir(exist_ok=True)

        # Sample Summary
        _ = write_table(grp, out_project / 'SampleSummary.csv', dry_run=args.dry_run)

        # Lane Summary
        mask = tables['main_lane_summary']['Lane'].isin(lanes)
        _ = write_table(tables['main_lane_summary'].loc[mask], out_project / 'LaneSummary.csv', dry_run=args.dry_run)

        # Top Unknown Barcodes
        mask = tables['stats_top_unknown_barcodes']['Lane'].isin(lanes)
        _ = write_table(tables['stats_top_unknown_barcodes'].loc[mask], out_project / 'TopUnknownBarcodes.csv', dry_run=args.dry_run)

    return 0


if __name__ == '__main__':
    res = main()
    sys.exit(res)

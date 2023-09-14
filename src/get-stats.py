#!/usr/bin/env python
# coding: utf-8

import pathlib
import multiqc
import json
import pandas as pd
import argparse
import sys

from sample_sheet import SampleSheet


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
    
    # row = {
    #     'Lane': '',
    #     'Project': '',
    #     'Sample': sd['SampleName'],
    #     'Barcode sequence': barcode_sequence,
    #     'PF Clusters': '{:,}'.format(sample_nreads),
    #     '% of the lane': '{:.2f}'.format(percent_lane),
    #     '% Perfect barcode': '{:.2f}'.format(percent_perfect_barcode),
    #     '% One mismatch barcode': '{:.2f}'.format(percent_mismatch_barcode),
    #     'Yield (Mbases)': '{:,}'.format(yield_mbases),
    #     '% PF Clusters': 'Always 100???',
    #     '% >= Q30 bases': '{:.2f}'.format(percent_q30),
    #     'Mean Quality Score': '{:.2f}'.format(mean_quality)
    # }
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

def main():

    # datadir = pathlib.Path('/Users/johnmiller/data/gensvc')
    # parentdir = datadir / 'processed/230908_A01770_0050_BHKWFVDRX3/1694438321'
    # samplesheetfile = parentdir / 'UTK_Hazen_kash_zgriffi3_230908-1694438500.csv'
    # statsdir = parentdir / 'fastq/Stats'
    # statsfile = statsdir / 'Stats.json'

    parser = argparse.ArgumentParser(
        prog='ParseStats',
        description='Parse stats from bcl2fastq and split by project.'
    )

    parser.add_argument('samplesheetfile')
    parser.add_argument('statsfile')

    args = parser.parse_args()

    with open(args.statsfile) as f:
        statsdata = json.load(f)

    # Get sample statistics.
    ss = SampleSheet(args.samplesheetfile)

    sample_df = pd.DataFrame([sample.to_json() for sample in ss.samples])

    demux_data = []
    for lane_number, lane in enumerate(statsdata['ConversionResults'], start=1):
        # print(lane_number)
        for sample in lane['DemuxResults']:
            # print(sample['SampleName'])
            demux_data.append(get_sample_stats(sd=sample, lc=lane))

    demux_df = pd.DataFrame(demux_data)

    foo = pd.merge(
        left=sample_df,
        right=demux_df,
        left_on='Sample_Name',
        right_on='Sample'
    )

    # [TODO] Match columns from report.
    for name, grp in foo.groupby('Sample_Project'):
        grp.to_csv(
            f'SequencingStatisticsSampleSummary-{name}.csv',
            index=False
        )


    # Get unknown barcodes
    top_u_barcodes = get_top_unknown_barcodes(statsdata, n=10)
    top_u_barcodes.to_csv('SequencingStatisticsTopUnknownBarcodes.csv', index=False)


if __name__ == '__main__':
    main()
    sys.exit()

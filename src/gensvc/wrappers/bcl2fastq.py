'''
Work with bcl2fastq and its output.

Summary stats
=============

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
import os
import warnings
import pandas as pd
import tempfile

from datetime import datetime

# Third party
# from sample_sheet import SampleSheet
# import multiqc

def summarize_sample_stats(sd, lc):
    '''
    DEPRECATED: These are accurate but I wasn't able to figure out how one of
    the stats in the summary reports was calculated, so I ultimately decided to
    extract the tables from those reports rather than calculate them.

    ---

    Use the ".../Stats/Stats.json" file to recapitulate the tables in "Reports/.../index.html".

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


def summarize_stats(stats_data, as_dataframe=True):
    '''
    Read stats from <bcl2fastq>/Stats/Stats.json and return summary data.
    '''
    summary_stats = []

    # Get per lane stats, starting with lane 1.
    for lane_number, lane in enumerate(stats_data['ConversionResults'], start=1):
        for sample in lane['DemuxResults']:
            summary_stats.append(
                summarize_sample_stats(sd=sample, lc=lane)
            )

    if as_dataframe:
        summary_stats = pd.DataFrame(summary_stats)

    return summary_stats


def get_top_unknown_barcodes(stats_data, n=10):
    top_ubarcodes = []
    for lane in stats_data['UnknownBarcodes']:
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


def extract_reports(fastqdir):
    tables = {}
    p = list(fastqdir.rglob('all/all/all/lane.html'))[0]
    data = pd.read_html(p)

    tables['main_flowcell_summary'] = data[1]
    tables['main_lane_summary'] = data[2]
    
    p = list(fastqdir.rglob('all/all/all/laneBarcode.html'))[0]
    data = pd.read_html(p)
    
    tables['samples_flowcell_summary'] = data[1]
    tables['samples_lane_summary'] = data[2]
    tables['samples_top_unknown_barcodes'] = data[3]

    return tables


def extract_stats(fastqdir):

    # Scrape tables from html files in the Reports directory.
    tables = extract_reports(fastqdir)

    with open(fastqdir / 'Stats/Stats.json') as f:
        stats_data = json.load(f)

    # Generate the lane summary table.
    tables['stats_lane_summary'] = summarize_stats(stats_data)

    # Generate unknown barcodes table.
    tables['stats_top_unknown_barcodes'] = get_top_unknown_barcodes(stats_data, n=10)

    return tables
    

def write_table(df, path, index=False, dry_run=False):
    if dry_run:
        print(df.to_string(index=index))
        return None
    else:
        return df.to_csv(path, index=index)


def write_summary_stats(tables, outdir, dry_run=False):
    # All Lanes: These files are the ones that the Genomics Core will want to
    # look at. They contain summary stats for all sequences in all lanes.
    # args.outdir.mkdir(parents=True, exist_ok=True)
    out_all = outdir / 'all'
    out_all.mkdir(parents=True, exist_ok=True)
    _ = write_table(tables['main_flowcell_summary'], out_all / 'FlowcellSummary.csv', dry_run=dry_run)
    _ = write_table(tables['main_lane_summary'], out_all / 'LaneSummary.csv', dry_run=dry_run)
    _ = write_table(tables['samples_lane_summary'], out_all / 'SampleSummary.csv', dry_run=dry_run)
    _ = write_table(tables['stats_top_unknown_barcodes'], out_all / 'TopUnknownBarcodes.csv', dry_run=dry_run)

    # Split Lanes: Each user's data is wholly contained in a subset of the
    # lanes. We only want to provide the summary stats relevant for the lanes
    # that contain that user's data.
    for name, grp in tables['samples_lane_summary'].groupby('Project'):
        if name == 'default':
            continue
        lanes = set(grp['Lane'])
        out_project = outdir / name
        out_project.mkdir(exist_ok=True)

        # Sample Summary
        _ = write_table(grp, out_project / 'SampleSummary.csv', dry_run=dry_run)

        # Lane Summary
        mask = tables['main_lane_summary']['Lane'].isin(lanes)
        _ = write_table(tables['main_lane_summary'].loc[mask], out_project / 'LaneSummary.csv', dry_run=dry_run)

        # Top Unknown Barcodes
        mask = tables['stats_top_unknown_barcodes']['Lane'].isin(lanes)
        _ = write_table(tables['stats_top_unknown_barcodes'].loc[mask], out_project / 'TopUnknownBarcodes.csv', dry_run=dry_run)

    return 0


def init_output_dir(procdir, runid):
    # if not procdir or not procdir.exists():
    #     raise ValueError(f'The root processed data directory must exist; you gave: "{procdir}"')
    if isinstance(procdir, pathlib.Path) and procdir.exists():
        return procdir / runid / datetime.now().strftime('%Y%m%dT%H%M%S') / '_bcl2fastq'
    else:
        return tempfile.mkdtemp()

def bcl2fastq(runfolder_dir=None, sample_sheet=None, output_dir=None, processing_threads=None):
    # Do not use os.cpu_count on a login node.
    # TODO Have the batch script get the number of cpus from the environment.
    params = dict(
        runfolder_dir=runfolder_dir,
        sample_sheet=sample_sheet or seqrun.path_to_samplesheet,
        output_dir=output_dir or init_output_dir(GENSVC_PROCDATA, seqrun.runid),
        processing_threads=processing_threads
    )

    if not params.get('runfolder_dir'):
        warnings.warn('runfolder_dir is required for bcl2fastq')

    cmd = []

    # Setup module.
    cmd.extend([
        'source /usr/share/Modules/init/bash ;',
        'module purge ;',
        'module load bcl2fastq2 ;',
        f'mkdir -pv {params["output_dir"]} ;',
        f'cp -v {params["sample_sheet"]} {params["output_dir"]} ;'
    ])

    # Re: passing commands to subprocess as str or list:
    # > args is required for all calls and should be a string, or a sequence of
    # > program arguments. Providing a sequence of arguments is generally
    # > preferred, as it allows the module to take care of any required escaping
    # > and quoting of arguments (e.g. to permit spaces in file names).
    # > ~ https://docs.python.org/3/library/subprocess.html
    # However, the simple-slurm module requires a str as input.
    line = [
        'bcl2fastq',
        f'--runfolder-dir {params["runfolder_dir"]}'
    ]
    if params.get('sample_sheet'):
        line.append(f'--sample-sheet {params["sample_sheet"]}')
    if params.get('output_dir'):
        line.append(f'--output-dir {params["output_dir"]}')
    if params.get('processing_threads'):
        line.append(f'--processing-threads {params["processing_threads"]}')
    line.append(' ;')
    cmd.append(' '.join(line))

    return '\n'.join(cmd)

class BCL2FastqData(ProcessedData):
    def __init__(self, rundir, runid=None, runfolder_dir=None, sample_sheet=None, output_dir=None, processing_threads=None, **kwargs):
        self._rundir = rundir
        self._runid = runid
        self._runfolder_dir = runfolder_dir
        self._sample_sheet_orig = sample_sheet
        self._sample_sheet_copy = None
        self._output_dir = output_dir
        self._processing_threads = processing_threads
        super().__init__(**kwargs)

# END

import json
import numpy as np
import os
import pandas as pd
import pathlib
import subprocess

from simple_slurm import Slurm

from gensvc.misc.config import config
from gensvc.data import illumina
from gensvc.core_facility.sequencing_run import SequencingRun

# Example sbatch directives:
# #SBATCH --job-name=bcl-convert-$(basename $bcl_input_directory)
# #SBATCH --account=ISAAC-UTK0192
# #SBATCH --partition=short
# #SBATCH --qos=short
# #SBATCH --ntasks=1
# #SBATCH --cpus-per-task=48
# #SBATCH --time=0-03:00:00
# #SBATCH --output=%x-%j.o
# #SBATCH --mail-type=ALL
# #SBATCH --mail-user=OIT_HPSC_Genomics@utk.edu

slurm = Slurm(
    job_name='bclconvert',
    account='ISAAC-UTK0192',
    nodes=1,
    ntasks=1,
    cpus_per_task=48,
    partition='short',
    qos='short',
    time='0-03:00:00',
    output='bclconvert-%j.o',
    mail_type='ALL',
    mail_user='bcs@utk.edu'
)
slurm.set_shell('/bin/bash -l')
slurm.add_cmd('set -e')
slurm.add_cmd('set -u')
slurm.add_cmd('set -o pipefail')
slurm.add_cmd('umask 002')
slurm.add_cmd('ulimit -n 16384')


class BCLConvert:
    '''
    Wrapper for Illumina BCL Convert command line tool. Non-standard options
    are accessed as attributes that are named with a trailing underscore, e.g.,
    `executable_filepath_`. Standard BCL Convert options are accessed as normal
    attributes. 

    Parameters
    ----------
    bcl_input_directory : str or pathlib.Path
    ...
    executable_filepath_ : str or pathlib.Path, optional
    parent_directory_ : str or pathlib.Path, optional
    sample_sheet:
        Default input folder

    Comments
    --------

    - A BCL Convert job requires a sample sheet. The sample sheet contains two
      things: sample metadata and settings for BCL Convert. This `BCLConvert`
      class should keep track of all the settings needed to run BCL Convert. It
      essentially 'owns' the sample sheet at the `sample_sheet` attribute. When
      this object is initialized, it should read settings from the sample
      sheet, and when any of the `run` methods are called, it should write the
      settings back to the sample sheet.
    '''
    def __init__(self,
            executable_filepath=None,
            bcl_input_directory=None,
            output_directory=None,
            sample_sheet=None,
            bcl_sampleproject_subdirectories=True,
            sample_name_column_enabled=True,
            output_legacy_stats=True
        ):
        self.executable_filepath = executable_filepath
        self.bcl_input_directory = bcl_input_directory
        self.output_directory = output_directory
        self.sample_sheet = sample_sheet
        self.bcl_sampleproject_subdirectories = bcl_sampleproject_subdirectories
        self.sample_name_column_enabled = sample_name_column_enabled
        self.output_legacy_stats = output_legacy_stats

    def __repr__(self):
        return self.cmd

    @property
    def executable_filepath(self):
        return self._executable_filepath

    @executable_filepath.setter
    def executable_filepath(self, value):
        if value is None:
            self._executable_filepath = pathlib.Path('bcl-convert')
        else:
            self._executable_filepath = pathlib.Path(value)

    @property
    def bcl_input_directory(self):
        return self._bcl_input_directory

    @bcl_input_directory.setter
    def bcl_input_directory(self, value):
        self._bcl_input_directory = value

    @property
    def output_directory(self):
        return self._output_directory

    @output_directory.setter
    def output_directory(self, value):
        self._output_directory = value

    @property
    def sample_sheet(self):
        return self._sample_sheet

    @sample_sheet.setter
    def sample_sheet(self, value):
        self._sample_sheet = value

    @property
    def bcl_sampleproject_subdirectories(self):
        return self._bcl_sampleproject_subdirectories

    @bcl_sampleproject_subdirectories.setter
    def bcl_sampleproject_subdirectories(self, value):
        if not isinstance(value, bool):
            raise ValueError("bcl_sampleproject_subdirectories must be a boolean value")
        self._bcl_sampleproject_subdirectories = value

    @property
    def sample_name_column_enabled(self):
        return self._sample_name_column_enabled

    @sample_name_column_enabled.setter
    def sample_name_column_enabled(self, value):
        if not isinstance(value, bool):
            raise ValueError("sample_name_column_enabled must be a boolean value")
        self._sample_name_column_enabled = value

    @property
    def output_legacy_stats(self):
        return self._output_legacy_stats

    @output_legacy_stats.setter
    def output_legacy_stats(self, value):
        if not isinstance(value, bool):
            raise ValueError("output_legacy_stats must be a boolean value")
        self._output_legacy_stats = value

    @property
    def cmdlist(self):
        cmd = [str(self.executable_filepath)]

        if self.bcl_input_directory:
            cmd += ['--bcl-input-directory', str(self.bcl_input_directory)]

        if self.output_directory:
            cmd += ['--output-directory', str(self.output_directory)]

        if self.sample_sheet:
            cmd += ['--sample-sheet', str(self.sample_sheet)]

        if self.bcl_sampleproject_subdirectories is False:
            cmd += ['--bcl-sampleproject-subdirectories', 'false']
        else:
            cmd += ['--bcl-sampleproject-subdirectories', 'true']

        if self.sample_name_column_enabled is False:
            cmd += ['--sample-name-column-enabled', 'false']
        else:
            cmd += ['--sample-name-column-enabled', 'true']

        if self.output_legacy_stats is False:
            cmd += ['--output-legacy-stats', 'false']
        else:
            cmd += ['--output-legacy-stats', 'true']

        return cmd

    @property
    def cmd(self):
        return ' '.join(self.cmdlist)

    def run(self):
        subprocess.run(self.cmdlist, check=True)

    def srun(self, *args, **kwargs):
        slurm.srun(self.cmdlist, *args, **kwargs)

    def sbatch(self, *args, **kwargs):
        slurm.sbatch(self.cmdlist, *args, **kwargs)


class BCLConvertReports:
    '''
    Reports/
    ├── Adapter_Cycle_Metrics.csv
    ├── Adapter_Metrics.csv
    ├── Demultiplex_Stats.csv
    ├── Demultiplex_Tile_Stats.csv
    ├── fastq_list.csv
    ├── Index_Hopping_Counts.csv
    ├── IndexMetricsOut.bin
    ├── legacy
    │   ├── Reports
    │   │   └── html
    │   │       ├── HCVTGDRX7
    │   │       │   ├── all
    │   │       │   │   └── all
    │   │       │   │       ├── all
    │   │       │   │       │   ├── laneBarcode.html
    │   │       │   │       │   └── lane.html
    │   │       │   │       └── unknown
    │   │       │   ├── default
    │   │       │   │   ├── all
    │   │       │   │   │   ├── all
    │   │       │   │   │   │   ├── laneBarcode.html
    │   │       │   │   │   │   └── lane.html
    │   │       │   │   │   └── unknown
    │   │       │   │   └── Undetermined
    │   │       │   │       ├── all
    │   │       │   │       │   ├── laneBarcode.html
    │   │       │   │       │   └── lane.html
    │   │       │   │       └── unknown
    │   │       │   │           ├── laneBarcode.html
    │   │       │   │           └── lane.html
    │   │       │   └── <Sample_Project>
    │   │       │       ├── all
    │   │       │       │   ├── all
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   └── unknown
    │   │       │       ├── <Sample_Name_1>
    │   │       │       │   ├── all
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   ├── ATCTCAGG+AGAGGATA
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   └── unknown
    │   │       │       ├── <Sample_Name_2>
    │   │       │       │   ├── all
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   ├── ATCTCAGG+TACTCCTT
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   └── unknown
    │   │       │       ├── <Sample_Name_3>
    │   │       │       │   ├── all
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   ├── ATCTCAGG+AGGCTTAG
    │   │       │       │   │   ├── laneBarcode.html
    │   │       │       │   │   └── lane.html
    │   │       │       │   └── unknown
    │   │       │       └── <Sample_Name_4>
    │   │       │           ├── all
    │   │       │           │   ├── laneBarcode.html
    │   │       │           │   └── lane.html
    │   │       │           ├── ATCTCAGG+CGGAGAGA
    │   │       │           │   ├── laneBarcode.html
    │   │       │           │   └── lane.html
    │   │       │           └── unknown
    │   │       ├── index.html
    │   │       ├── Report.css
    │   │       └── tree.html
    │   └── Stats
    │       ├── AdapterTrimming.txt
    │       ├── ConversionStats.xml
    │       ├── DemultiplexingStats.xml
    │       ├── DemuxSummaryF1L1.txt
    │       ├── DemuxSummaryF1L2.txt
    │       ├── FastqSummaryF1L1.txt
    │       ├── FastqSummaryF1L2.txt
    │       └── Stats.json
    ├── Quality_Metrics.csv
    ├── Quality_Tile_Metrics.csv
    ├── RunInfo.xml
    ├── SampleSheet.csv
    └── Top_Unknown_Barcodes.csv
    '''
    def __init__(self, reports_dir):
        self.reports_dir = pathlib.Path(reports_dir)

    def ls(self):
        return sorted([f.name for f in self.reports_dir.iterdir()])

    def load_adapter_cycle_metrics(self):
        return pd.read_csv(self.reports_dir / "Adapter_Cycle_Metrics.csv")

    def load_adapter_metrics(self):
        return pd.read_csv(self.reports_dir / "Adapter_Metrics.csv")

    def load_demultiplex_stats(self):
        return pd.read_csv(self.reports_dir / "Demultiplex_Stats.csv")

    def load_demultiplex_tile_stats(self):
        return pd.read_csv(self.reports_dir / "Demultiplex_Tile_Stats.csv")

    def load_fastq_list(self):
        return pd.read_csv(self.reports_dir / "fastq_list.csv")

    def load_index_hopping_counts(self):
        return pd.read_csv(self.reports_dir / "Index_Hopping_Counts.csv")

    def load_quality_metrics(self):
        return pd.read_csv(self.reports_dir / "Quality_Metrics.csv")

    def load_quality_tile_metrics(self):
        return pd.read_csv(self.reports_dir / "Quality_Tile_Metrics.csv")

    def load_sample_sheet(self):
        raise NotImplementedError("SampleSheet.csv parsing not implemented.")

    def load_top_unknown_barcodes(self):
        return pd.read_csv(self.reports_dir / "Top_Unknown_Barcodes.csv")

    def get_legacy_stats(self):
        return BCLConvertLegacyReports(self.reports_dir / "legacy")


class BCLConvertLegacyReports:
    def __init__(self, legacy_dir):
        self.legacy_dir = pathlib.Path(legacy_dir)


def extract_tables(statsdir):
    '''
    Helper function for `extract_tables_from_legacy_stats()`.
    '''
    tables = {}
    p = list(statsdir.rglob('all/all/all/lane.html'))[0]
    data = pd.read_html(p)
    # len(data)

    # data[0]
    tables['main_flowcell_summary'] = data[1]
    tables['main_lane_summary'] = data[2]

    p = list(statsdir.rglob('all/all/all/laneBarcode.html'))[0]
    data = pd.read_html(p)
    # len(data)

    # data[0]
    try:
        # print(data[1].columns)
        tables['samples_flowcell_summary'] = data[1]
    except:
        pass
    try:
        # print(data[2].columns)
        tables['samples_lane_summary'] = data[2]
    except:
        pass
    try:
        # print(data[3].columns)
        tables['samples_top_unknown_barcodes'] = data[3]
    except:
        pass

    return tables


def get_sample_stats(sample_stats, lane_stats):
    '''
    sample_stats : Sample Demultiplex Results
    lane_stats : Lane Conversion Results
    '''

    barcode_sequence = sample_stats['IndexMetrics'][0]['IndexSequence']
    sample_nreads = sample_stats['NumberReads']
    if sample_nreads > 0:
        lane_nclusters = lane_stats['TotalClustersPF']
        percent_lane = (sample_nreads / lane_nclusters)*100
        percent_perfect_barcode = sample_stats['IndexMetrics'][0]['MismatchCounts']['0'] / sample_stats['NumberReads'] * 100
        percent_mismatch_barcode = sample_stats['IndexMetrics'][0]['MismatchCounts']['1'] / sample_stats['NumberReads'] * 100
        yield_mbases = round(sample_stats['Yield'] / 1000000)

        # Percent bases w/ Q30
        y0 = sample_stats['ReadMetrics'][0]['YieldQ30'] / sample_stats['ReadMetrics'][0]['Yield']
        y1 = sample_stats['ReadMetrics'][1]['YieldQ30'] / sample_stats['ReadMetrics'][1]['Yield']
        percent_q30 = ((y0+y1)/2)*100

        # Mean Quality score
        q0 = sample_stats['ReadMetrics'][0]['QualityScoreSum'] / sample_stats['ReadMetrics'][0]['Yield']
        q1 = sample_stats['ReadMetrics'][1]['QualityScoreSum'] / sample_stats['ReadMetrics'][1]['Yield']
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
        'Sample': sample_stats['SampleName'],
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
    '''
    Helper function for `extract_tables_from_legacy_stats()`.
    '''
    demux_data = []
    for lane_number, lane in enumerate(statsdata['ConversionResults'], start=1):
        # print(lane_number)
        for sample in lane['DemuxResults']:
            # print(sample['SampleName'])
            demux_data.append(get_sample_stats(sample_stats=sample, lane_stats=lane))
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


def extract_tables_from_legacy_stats(bclconvert_directory):
    '''
    Extract stats tables from a BCL Convert legacy reports.
    '''
    statsdir = bclconvert_directory / 'Reports/legacy'
    # logging.debug(f'statsdir={statsdir}')

    # Scrape tables from html files in the Reports directory.
    tables = extract_tables(statsdir)
    # logging.debug(f'Found {len(tables)} tables.')

    # Load stats data.
    with open(statsdir / 'Stats/Stats.json') as f:
        statsdata = json.load(f)

    tables['stats_lane_summary'] = make_samples_lane_summary(statsdata)

    # Get unknown barcodes
    tables['stats_top_unknown_barcodes'] = get_top_unknown_barcodes(statsdata, n=10)

    return tables


def report_suggested_barcodes(demultiplex_stats, top_unknown_barcodes):
    '''
    Check each index ("barcode") for each sample in the demultiplex stats
    against the top unknown barcodes. If the index matches any of the unknown
    barcodes in any reverse-complemented form, then report it along with the
    counts of the original and the counts of the unknown barcode.

    [TODO] Break up this function. Also, it should iterate over each
    'Sample_Project' and set 'Sample_Name' to 'NA' if there are no samples
    matching an unknown barcode.

    Parameters
    ----------
    demultiplex_stats : pd.DataFrame
        Lane         SampleID  Sample_Project      Sample_Name              Index    # Reads  # Perfect Index Reads  # One Mismatch Index Reads  # Two Mismatch Index Reads  % Reads  % Perfect Index Reads  % One Mismatch Index Reads  % Two Mismatch Index Reads
           1  Sample_1_251001  Project_251001  Sample_1_251001  ATCTCAGG-TATCCTCT         92                      0                          92                           0      0.0                    0.0                         1.0                         0.0
           1  Sample_2_251001  Project_251001  Sample_2_251001  ATCTCAGG-AAGGAGTA         40                      0                          40                           0      0.0                    0.0                         1.0                         0.0
           1  Sample_3_251001  Project_251001  Sample_3_251001  ATCTCAGG-CTAAGCCT          1                      0                           1                           0      0.0                    0.0                         1.0                         0.0
           1  Sample_4_251001  Project_251001  Sample_4_251001  ATCTCAGG-TCTCTCCG          0                      0                           0                           0      0.0                    0.0                         0.0                         0.0
           1     Undetermined    Undetermined     Undetermined                NaN  500230507              500230507                           0                           0      1.0                    1.0                         0.0                         0.0
           2 ...
    top_unknown_barcodes: pd.DataFrame
        Lane     index    index2    # Reads  % of Unknown Barcodes  % of All Reads
           1  ATCTCAGG  CGGAGAGA  125580963               0.251046        0.251046
           1  ATCTCAGG  TACTCCTT  112136682               0.224170        0.224170
           1  ATCTCAGG  AGGCTTAG   92311141               0.184537        0.184537
           1  ATCTCAGG  AGAGGATA   86030827               0.171982        0.171982
           1  GGGGGGGG  AGATCTCG    7616589               0.015226        0.015226
           ...

    Returns
    -------
    pd.DataFrame
        Lane  Sample_Project      Sample_Name            Barcode  Read_count Reverse_complement    Unknown_barcode  Unknown_barcode_ID  Unknown_read_count  Diff_count  Log2_FoldChange
           1  Project_251001  Sample_1_251001  ATCTCAGG-TATCCTCT          92                 i5  ATCTCAGG-AGAGGATA                   1            86030827    86030735            19.83
           2  Project_251001  Sample_1_251001  ATCTCAGG-TATCCTCT          93                 i5  ATCTCAGG-AGAGGATA                   1            85643023    85642930            19.81
           1  Project_251001  Sample_2_251001  ATCTCAGG-AAGGAGTA          40                 i5  ATCTCAGG-TACTCCTT                   2           112136682   112136642            21.42
           2  Project_251001  Sample_2_251001  ATCTCAGG-AAGGAGTA          46                 i5  ATCTCAGG-TACTCCTT                   2           112999614   112999568            21.23
           1  Project_251001  Sample_3_251001  ATCTCAGG-CTAAGCCT           1                 i5  ATCTCAGG-AGGCTTAG                   3            92311141    92311140            26.46
           ...
    '''
    # Data dictionary for output.
    dictionary = {
        'Barcode': 'The given barcode sequence from the sample sheet.',
        'i7': 'The i7 portion of the given barcode sequence.',
        'i5': 'The i5 portion of the given barcode sequence.',
        'Diff_count': '`Unknown count` - `PF Clusters`',
        'Lane': 'Lane on flow cell.',
        'Log2_FoldChange': 'log2(`Unknown count` / `PF Clusters`) [Denominator is set to 1 if `PF Clusters` is 0.]',
        'Reverse_complement': 'Method used to match the given barcode to the unknown barcode. One of: "i7", "i5", "both", "full".',
        'SampleID': 'Name of sample.',
        'Sample_Name': 'Name of sample.',
        'Sample_Project': 'Name of project (same as `ProjectName` or `Sample_Project` column).',
        'Unknown_barcode': 'A barcode sequence not present in the sample sheet.',
        'Unknown_i7': 'The i7 portion of the `Unknown barcode`.',
        'Unknown_i5': 'The i5 portion of the `Unknown barcode`.',
        'Unknown_barcode_ID': 'Unique integer identifier for `Unknown barcode`.',
        'Unknown_read_count': 'Count of reads assigned to that `Unknown barcode.`',
        'Read_count': 'Number of reads (aka, "PF Clusters", Clusters passing filter, in legacy stats).'
    }
    # Store list of records for output.
    records = []
    # Keep track of barcodes with a unique integer ID.
    barcode_ids = {}
    bi = 1
    # First, iterate over each lane.
    lanes = sorted(set(demultiplex_stats['Lane']))
    for lane in lanes:
        # Get the sample summaries (demultiplex stats) for that lane.
        summary = demultiplex_stats.query(f'Lane=={lane}')
        # Get the "unknown" barcodes for that lane.
        unknown_barcodes_lane = top_unknown_barcodes.query(f'Lane=={lane}')
        # Next, iterate over the samples in that lane.
        for i, sample in summary.iterrows():
            if sample['SampleID'] == 'Undetermined':
                continue
            # You want to know if this sequence matches the *truth*, which is,
            # unfortunately, named "Unknown barcode".
            b2 = illumina.Barcode(sequence=sample['Index'])
            rec = {
                'Lane': sample['Lane'],
                'SampleID': sample['SampleID'],
                'Barcode': sample['Index'],
                'i7': str(b2.i7),
                'i5': str(b2.i5),
                'Read_count': sample['# Reads']
            }
            if 'Sample_Project' in sample.index:
                rec['Sample_Project'] = sample['Sample_Project']

            # Then, iterate over the "unknown" barcodes, one at a time.
            for j, unknown in unknown_barcodes_lane.iterrows():
                # This is the *real* sequence. You want to check if there is a
                # variation of the sample index that matches this one.
                b1 = illumina.Barcode(i7=unknown['index'], i5=unknown['index2'])
                matches = illumina.compare_barcodes(b1, b2)
                for method, match in matches.items():
                    if str(match) not in barcode_ids:
                        barcode_ids[str(match)] = bi
                        bi += 1
                    tmp = {**rec}
                    tmp['Reverse_complement'] = method
                    tmp['Unknown_barcode'] = str(b1)
                    tmp['Unknown_i7'] = str(b1.i7)
                    tmp['Unknown_i5'] = str(b1.i5)
                    tmp['Unknown_barcode_ID'] = barcode_ids[str(match)]
                    tmp['Unknown_read_count'] = unknown['# Reads']
                    tmp['Diff_count'] = unknown['# Reads'] - sample['# Reads']
                    tmp['Log2_FoldDiff'] = np.log2(unknown['# Reads'] / max(sample['# Reads'], 1)).round(2)
                    records.append(tmp)

    records = sorted(records, key=lambda d: (d['Unknown_barcode_ID'], d['Lane']))

    df = pd.DataFrame(records)

    return df


def cli(args):

    if os.path.isdir(args.runid_or_rundir):
        runid = os.path.basename(args.runid_or_rundir)
        seqrun = SequencingRun(runid, path_to_rundir=args.runid_or_rundir)
    else:
        runid = args.runid_or_rundir
        seqrun = SequencingRun(runid)

    if args.bcl_input_directory is None:
        bcl_input_directory = seqrun.rundir.path
    else:
        bcl_input_directory = args.bcl_input_directory

    if args.output_directory is None:
        # # Use defaults.
        # if not config.GENSVC_PROCDATA.exists():
        #     raise ValueError(f"GENSVC_PROCDATA directory does not exist: {config.GENSVC_PROCDATA}")
        # output_parent = config.GENSVC_PROCDATA / runid
        output_directory = seqrun.procdir.path / 'BCLConvert'
        job_file = seqrun.procdir.path / 'bclconvert.sh'
    else:
        output_directory = args.output_directory
        job_file = None

    if args.sample_sheet is None:
        sample_sheet = seqrun.rundir.path_to_samplesheet
    else:
        sample_sheet = args.sample_sheet

    # args.output_directory = output_directory
    bclconvert = BCLConvert(
        executable_filepath=args.path_to_bclconvert_exe,
        bcl_input_directory=bcl_input_directory,
        output_directory=output_directory,
        sample_sheet=sample_sheet,
        bcl_sampleproject_subdirectories=args.bcl_sampleproject_subdirectories,
        sample_name_column_enabled=args.sample_name_column_enabled,
        output_legacy_stats=args.output_legacy_stats
    )

    # Debug.
    # print(args)
    # print(config)
    # print(config.GENSVC_PROCDATA, type(config.GENSVC_PROCDATA))
    # print(output_parent, type(output_parent))
    # print(output_directory, type(output_parent))

    if args.dump:
        print(slurm)
        print(bclconvert)

    if args.run:
        output_directory.parent.mkdir(parents=False, exist_ok=True)
        bclconvert.run()
    elif args.srun:
        output_directory.parent.mkdir(parents=False, exist_ok=True)
        slurm.srun(bclconvert.cmd)
    elif args.sbatch:
        # Submit a job with sbatch. simple_slurm will create the job file at
        # `job_file` and the output file will be created in the current working
        # directory in the same manner as submitting with sbatch CLI.
        output_directory.parent.mkdir(parents=False, exist_ok=True)
        if job_file is not None:
            # Coerce Path obj to string.
            slurm.sbatch(bclconvert.cmd, job_file=str(job_file))
        else:
            slurm.sbatch(bclconvert.cmd)

    return 0

# END

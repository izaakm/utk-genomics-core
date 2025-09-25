import pathlib
import json
import os
import pandas as pd
import subprocess

from simple_slurm import Slurm

from gensvc.misc.config import config

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
    ...

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
        self._executable_filepath = executable_filepath
        self._bcl_input_directory = bcl_input_directory
        self._output_directory = output_directory
        self._sample_sheet = sample_sheet
        self._bcl_sampleproject_subdirectories = bcl_sampleproject_subdirectories
        self._sample_name_column_enabled = sample_name_column_enabled
        self._output_legacy_stat = output_legacy_stats

    def __repr__(self):
        return self.cmd

    @property
    def executable_filepath_(self):
        return self._executable_filepath

    @executable_filepath_.setter
    def executable_filepath_(self, value):
        self._executable_filepath = value

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
        return self._output_legacy_stat

    @output_legacy_stats.setter
    def output_legacy_stats(self, value):
        if not isinstance(value, bool):
            raise ValueError("output_legacy_stats must be a boolean value")
        self._output_legacy_stat = value

    @property
    def cmdlist(self):
        cmd = [str(self._executable_filepath)]

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



def cli(args):

    if args.bcl_input_directory is None:
        raise ValueError("bcl_input_directory is required")
    run_id = args.bcl_input_directory.name

    if args.output_directory is None:
        # Use defaults.
        if not config.GENSVC_PROCDATA.exists():
            raise ValueError(f"GENSVC_PROCDATA directory does not exist: {config.GENSVC_PROCDATA}")
        output_parent = config.GENSVC_PROCDATA / run_id
        output_directory =  output_parent / 'BCLConvert'
        job_file = output_parent / 'bclconvert.sh'
    else:
        output_directory = args.output_directory
        output_parent = output_directory.parent
        job_file = None

    args.output_directory = output_directory
    # print(args)
    
    bclconvert = BCLConvert(
        executable_filepath=args.path_to_bclconvert_exe,
        bcl_input_directory=args.bcl_input_directory,
        output_directory=args.output_directory,
        sample_sheet=args.sample_sheet,
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
        output_parent.mkdir(parents=True, exist_ok=True)
        bclconvert.run()
    elif args.srun:
        output_parent.mkdir(parents=True, exist_ok=True)
        slurm.srun(bclconvert.cmd)
    elif args.sbatch:
        output_parent.mkdir(parents=True, exist_ok=True)
        slurm.sbatch(bclconvert.cmd, job_file=str(job_file))

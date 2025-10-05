'''
>>> import importlib
>>> import pathlib
>>> from gensvc.misc import sequencing_run
>>> path = pathlib.Path('/lustre/isaac/proj/UTK0192/gensvc/NovaSeqRuns/241216_A01770_0089_BHT2C5DSXC/')
>>> seqrun = sequencing_run.IlluminaSequencingData(path)
>>> print(seqrun.path_to_samplesheet)
>>> seqrun.find_samplesheet()
>>> print(seqrun.path_to_samplesheet)
>>> print(seqrun.samplesheet)
>>> print(seqrun.samplesheet.path)
>>> print(seqrun.samplesheet.data)
'''


import csv
import hashlib
import itertools
import logging
import os
import pandas as pd
import pathlib
import re
import sys
import warnings

from io import StringIO
from datetime import datetime
from gensvc.misc import utils
from gensvc.data import base

# Hamming distances
from scipy.spatial import distance
# from sklearn.metrics import pairwise_distances
from Bio.Seq import Seq


_instrument_id ={
    'FS10003266': 'iSeq',
    'M04398': 'MiSeq',
    'VL00838': 'NextSeq',
    'A01770': 'NovaSeq'
}

_instruments = sorted(_instrument_id.values())


# Example run IDs:
# NextSeq: 240924_VL00838_3_AAG5VFFM5
# NovaSeq: 240820_A01770_0078_AHHMWLDRX5
# iSeq: 20250124_FS10003266_2_BWB90518-0813
re_runid = re.compile(r'\d{6,8}_[A-Z0-9]{6,}_\d{1,}_[-A-Z0-9]{1,}')
regex_runid = re.compile(r'[^\/]*\d{6,8}[^\/]*')  # DEPRECATED

# Sample sheet section header.
re_section = re.compile(r'^\[\s*(\w+)\s*]')


logger = logging.getLogger(__name__)


def is_runid(runid):
    if not isinstance(runid, str):
        return False
    if re_runid.match(runid):
        return True
    else:
        return False


def parse_sample_sheet(path):
    '''
    Parse an Illumina Sample Sheet.

    Parameters
    ----------
    path : str or pathlib.Path

    Returns
    -------
    dict
        A dictionary of sections, where each section is a list of lines.
    '''
    # header = re.compile(r'^\[\s*Header\s*]')
    content = {
        'sections': [],
    }
    lines = []
    key = None

    with open(path) as f:
        for line in f:
            sec = re_section.match(line)
            if sec:
                # New section
                # print(sec.group(0), sec.group(1))
                content['sections'].append(sec.group(1))
                if key and lines:
                    content[key] = lines
                key = sec.group(1)
                lines = []
            else:
                if line.strip():
                    lines.append(line.strip())
        else:
            if key and lines:
                content[key] = lines
    return content


def read_sample_sheet(path, version='infer'):
    content = parse_sample_sheet(path)

    if version == 'infer':
        if 'BCLConvert_Settings' in content:
            version = 2
        else:
            version = 1

    if version == 1:
        return SampleSheetv1(path=path, content=content)
    elif version == 2:
        return SampleSheetv2(path=path, content=content)
    else:
        raise ValueError(f'`version` must be `1` or `2`, you gave "{version}"')


def parse_list_section(lines, name=None):
    data = []
    for line in lines:
        if line.strip(' ,'):
            # Remove whitespace and trailing commas.
            data.append(line.strip().rstrip(','))
    return ListSection(data, name=name)


def parse_dict_section(lines, name=None, cls=None):
    if cls is None:
        cls = DictSection
    data = {}
    for line in lines:
        if not line:
            continue
        key, val, *_ = line.split(',')
        data[key] = val
    return cls(data, name=name)


def parse_table_section(lines, name=None):
    if lines:
        columns = lines.pop(0).split(',')
        rows = [line.split(',') for line in lines]
        data = pd.DataFrame(rows, columns=columns)
        # Convert whitespece to NaN.
        data = data.replace(r'^\s*$', pd.NA, regex=True)
        # Drop rows.
        data = data.dropna(axis=0, how='all')
        # Drop columns.
        data = data.dropna(axis=1, how='all')
    else:
        # Return an empty DataFrame for constistency with `parse_dict_section`.
        data = pd.DataFrame()
    return TableSection(data, name=name)


def looks_like_samplesheet(path):
    # print('Checking path ...')
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)

    if not path.is_file():
        # print('Not a file.')
        return False

    if path.name == 'SampleSheet.csv':
        return True
    elif path.read_text().startswith('[Header]'):
        return True
    else:
        return False


# def samples_to_dataframe(samplesheet):
#     return pd.DataFrame([s.to_json() for s in samplesheet.samples])


def get_sample_project(df, samples_col="Sample_ID", project_col="Sample_Project"):
    '''
    Get a mapping of Sample IDs to Project IDs from the sample sheet.

    Parameters
    ----------
    df : pandas.DataFrame
        The sample sheet data.
    samples_col : str
        The column name for Sample IDs.
    project_col : str
        The column name for Project IDs.

    Returns
    -------
    dict
        A dictionary mapping Sample IDs to Project IDs.
    
    Example
    -------

    >>> mapper = get_sample_project(
    >>>     samplesheet.Cloud_Data.data,
    >>>     project_col='ProjectName',
    >>> )
    '''
    return df.set_index(samples_col)[project_col].to_dict()


def set_sample_project(df, mapper, samples_col='Sample_ID', project_col='Sample_Project'):
    '''
    Set the Sample_Project column in the sample sheet data (in place).

    Parameters
    ----------
    df : pandas.DataFrame
        The sample sheet data.
    mapper : dict
        A dictionary mapping Sample IDs to Project IDs.
    samples_col : str
        The column name for Sample IDs.

    Returns
    -------
    None

    Example
    -------

    >>> mapper = get_sample_project(
    >>>     samplesheet.Cloud_Data.data,
    >>>     project_col='ProjectName',
    >>> )
    >>> set_sample_project(
    >>>     samplesheet.BCLConvert_Data.data,
    >>>     mapper
    >>> )
    '''
    df[project_col] = df[samples_col].map(mapper)
    return None


def verify_sample_id(df, samples_col='Sample_ID'):
    if not df[samples_col].is_unique:
        is_dupe = df[samples_col].duplicated(keep=False)
        dupes = df.loc[is_dupe, samples_col].to_list()
        raise ValueError(f'Found duplicate Sample IDs: {dupes}')
    return True


def verify_sample_project(df, project_col='Sample_Project'):
    if df[project_col].isin(['all', 'default']).any():
        # See https://knowledge.illumina.com/software/general/software-general-reference_material-list/000003710
        is_bad = df[project_col].isin(['all', 'default'])
        bad = df.loc[is_bad, project_col].drop_duplicates().to_list()
        raise ValueError(f'Found illegal Project IDs: {bad}')
    return True


def get_duplicate_indexes(df, index1_col='index', index2_col='index2', use_lane=True, sort=True):
    '''
    Get samples with duplicate indexes.

    Parameters
    ----------
    df : pandas.DataFrame
        The sample data.
    index1_col, index2_col : str
        The column names for the index1 and index2 columns.
    use_lane : bool
        Whether to include the Lane column in the duplicate check.
    '''
    if use_lane and 'Lane' in df.columns:
        cols = ['Lane', index1_col, index2_col]
    else:
        cols = [index1_col, index2_col]
    mask = df.duplicated(subset=cols, keep=False)
    dupes = df.loc[mask].copy()
    if sort:
        dupes = dupes.sort_values(cols)
    return dupes


def hamming(u, v):
    '''

    Examples
    --------
    >>> s1 = "GAGCCTACTAACGGGAT"
    >>> s2 = "CATCGTAATGACGGCCT"
    >>> hamming(s1, s2)
    7
    '''
    if len(u) == len(v):
        return round(
            distance.hamming(list(u), list(v)) * len(u)
        )
    else:
        raise ValueError('Sequences are not the same length.')


def pairwise_hamming_distance(U, V=None, reverse_complement=False):
    if V is None:
        combos = itertools.combinations(U, 2)
    else:
        combos = itertools.product(U, V)
    d = []
    for u, v in combos:
        d.append({'u': u, 'v': v, 'hamming': hamming(u, v)})
        if reverse_complement:
            # I think you only need to check the reverse complement of one of the sequences.
            d.append({'u': u, 'v': v, 'hamming': hamming(u.reverse_complement(), v), 'reverse_complement': 0})
            d.append({'u': u, 'v': v, 'hamming': hamming(u, v.reverse_complement()), 'reverse_complement': 1})
    return d


class ListSection:
    def __init__(self, data, name=None):
        '''
        data : list
        '''
        self._data = data
        self._name = name

    def __repr__(self):
        return f'{self.name}({self.data.__repr__()})'

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        if not isinstance(value, list):
            raise ValueError('Data must be a list')
        self._data = value

    @property
    def name(self):
        return self._name

    def to_csv(self, *args, file=None, **kwargs):
        text = f'[{self.name}]\n'
        text += '\n'.join(self.data) + '\n\n'
        if file is None:
            return text
        else:
            print(text, file=file)


class DictSection:
    def __init__(self, data, name=None):
        '''
        data : dict
        '''
        self._data = data
        self._name = name

    def __repr__(self):
        return f'{self.name}({self.data.__repr__()})'

    def __getitem__(self, *args, **kwargs):
        return self.data.__getitem__(*args, **kwargs)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        if not isinstance(value, dict):
            raise ValueError('Data must be a dictionary')
        self._data = value

    @property
    def name(self):
        return self._name

    def to_csv(self, *args, file=None, **kwargs):
        text = f'[{self.name}]\n'
        for key, val in self.data.items():
            text += f'{key},{val}\n'
        text += '\n'
        if file is None:
            return text
        else:
            print(text, file=file)


class SettingsSection(DictSection):
    '''
    Generic settings that can be used in both v1 and v2 sample sheets.
    '''
    @property
    def CreateFastqForIndexReads(self):
        return self.data.get('CreateFastqForIndexReads', None)

    @CreateFastqForIndexReads.setter
    def CreateFastqForIndexReads(self, value):
        '''
        Generating FASTQs for index reads is off by default, add the sample
        sheet setting with a value of 1 to enable. When an index read is
        specified as a UMI with OverrideCycles, the UMI read will be output to
        a FASTQ file. This feature introduced in BCL Convert version 3.7.5

        Sample Sheet: CreateFastqForIndexReads, 0 or 1 (default 0)

        [ref]: https://knowledge.illumina.com/software/general/software-general-reference_material-list/000003710
        '''
        if not isinstance(value, bool) and not value == 0 and not value == 1:
            raise ValueError('CreateFastqForIndexReads must be a boolean')
        if value:
            self.data['CreateFastqForIndexReads'] = 1
        else:
            self.data['CreateFastqForIndexReads'] = 0


class BCLConvertSettingsSection(SettingsSection):
    '''
    Settings for BCL Convert v4.2.7. See [ref1] for details.

    Attributes
    ----------
    SoftwareVersion : str
        Required. The version of the software to be used to perform BCL
        conversion on the Sample_ID's that only exist in the BCL Convert Data
        section of the sample sheet., The version is specified using all three
        integers included in the DRAGEN version name. For example, 1.0.0.
        See [ref1] for details.

    AdapterRead1 : str
        Optional. The sequence of the Read 1 adapter to be masked or trimmed.
        To trim multiple adapters, separate the sequences with a plus sign (+)
        indicating independent adapters that must be independently assessed for
        masking or trimming for each read. Characters must be A, C, G, or T.

        A value of na (case insensitive) must be used if:

        - the AdapterRead1 field is placed in the Data section of the Sample Sheet, and
        - no adapter trimming is desired for the sample

        See [ref1] for details.

    AdapterRead2 : str

        Optional. See description of AdapterRead1, applied to AdapterRead2.

        See [ref1] for details.

    BarcodeMismatchesIndex1 : int, NA

        Optional. Specifies barcode mismatch tolerance for Index 1. Possible
        values are 0, 1, or 2, or na. The default value is 1. Only allowed if
        Index is specified in RunInfo.xml file and in the Reads section of the
        Sample Sheet.

        A value of na must be used if:

        - BarcodeMismatchesIndex1 exists in the Data section of the Sample Sheet, and
        - OverrideCycles exist in the data section, and
        - Index 1 is masked out for the sample in the OverrideCycles

        See [ref1] for details.

    BarcodeMismatchesIndex2 : int, NA

        Optional. See description of BarcodeMismatchesIndex1, applied to BarcodeMismatchesIndex2.

        See [ref1] for details.

    OverrideCycles : str

        Optional. Specifies the sequencing and indexing cycles to be used when
        processing the sequencing data. See [ref1] for details.

        See [ref1] for details.

    FastqCompressionFormat : str

        Optional. The compression format for the FASTQ output files. Allowed
        values are gzip or dragen.

        See [ref1] for details.

    NoLaneSplitting : bool

        Optional. If set to true, output all lanes of a flow cell to the same
        FASTQ files consecutively.

        See [ref1] for details.

    CreateFastqForIndexReads: bool

        Optional. Generating FASTQs for index reads is off by default, add the
        sample sheet setting with a value of 1 to enable. When an index read is
        specified as a UMI with OverrideCycles, the UMI read will be output to
        a FASTQ file. This feature introduced in BCL Convert version 3.7.5

        See [ref2] for details.


    Examples
    --------

    >>> d = {'SoftwareVersion': '4.2.7', 'OverrideCycles': 'Y300;I12;Y300', 'BarcodeMismatchesIndex1': '0', 'FastqCompressionFormat': 'gzip', 'NoLaneSplitting': 'true'}
    >>> sec = BCLConvertSettingsSection(d)
    >>> sec.CreateFastqForIndexReads = True
    >>> print(sec)
    BCLConvert_Settings({'SoftwareVersion': '4.2.7', 'OverrideCycles': 'Y300;I12;Y300', 'BarcodeMismatchesIndex1': '0', 'FastqCompressionFormat': 'gzip', 'NoLaneSplitting': 'true', 'CreateFastqForIndexReads': 0})


    [ref1]: https://help.connected.illumina.com/run-set-up/overview/instrument-settings/nextseq-1000-2000-settings#tab-id-4.2.7
    [ref2]: https://knowledge.illumina.com/software/general/software-general-reference_material-list/000003710
    '''

    @property
    def name(self):
        return 'BCLConvert_Settings'

    @property
    def SoftwareVersion(self):
        return self.data.get('SoftwareVersion', None)

    @SoftwareVersion.setter
    def SoftwareVersion(self, value):
        if not isinstance(value, str):
            raise ValueError('SoftwareVersion must be a string')
        self.data['SoftwareVersion'] = value

    @property
    def AdapterRead1(self):
        return self.data.get('AdapterRead1', None)

    @AdapterRead1.setter
    def AdapterRead1(self, value):
        if not isinstance(value, str):
            raise ValueError('AdapterRead1 must be a string')
        self.data['AdapterRead1'] = value

    @property
    def AdapterRead2(self):
        return self.data.get('AdapterRead2', None)

    @AdapterRead2.setter
    def AdapterRead2(self, value):
        if not isinstance(value, str):
            raise ValueError('AdapterRead2 must be a string')
        self.data['AdapterRead2'] = value

    @property
    def BarcodeMismatchesIndex1(self):
        return self.data.get('BarcodeMismatchesIndex1', None)

    @BarcodeMismatchesIndex1.setter
    def BarcodeMismatchesIndex1(self, value):
        if value not in [0, 1, 2, 'na']:
            raise ValueError('BarcodeMismatchesIndex1 must be 0, 1, 2, or "na"')
        self.data['BarcodeMismatchesIndex1'] = value

    @property
    def BarcodeMismatchesIndex2(self):
        return self.data.get('BarcodeMismatchesIndex2', None)

    @BarcodeMismatchesIndex2.setter
    def BarcodeMismatchesIndex2(self, value):
        if value not in [0, 1, 2, 'na']:
            raise ValueError('BarcodeMismatchesIndex2 must be 0, 1, 2, or "na"')
        self.data['BarcodeMismatchesIndex2'] = value

    @property
    def OverrideCycles(self):
        return self.data.get('OverrideCycles', None)

    @OverrideCycles.setter
    def OverrideCycles(self, value):
        if not isinstance(value, str):
            raise ValueError('OverrideCycles must be a string')
        self.data['OverrideCycles'] = value

    @property
    def FastqCompressionFormat(self):
        return self.data.get('FastqCompressionFormat', None)

    @FastqCompressionFormat.setter
    def FastqCompressionFormat(self, value):
        if value not in ['gzip', 'dragen']:
            raise ValueError('FastqCompressionFormat must be "gzip" or "dragen"')
        self.data['FastqCompressionFormat'] = value

    @property
    def NoLaneSplitting(self):
        return self.data.get('NoLaneSplitting', None)

    @NoLaneSplitting.setter
    def NoLaneSplitting(self, value):
        '''
        Concatenation of FASTQ files separated by lane can be done by enabling
        this setting. FASTQs will be output with the naming convention
        <Sample_ID>_S#_<R or I>#_001.fastq.gz (no L00# included). Reports will
        be generated with values separated by lane. Command line option
        introduced in BCL Convert version 3.7.5. Sample sheet setting
        introduced in BCL Convert 3.8. Command line and sample sheet settings
        must be consistent.

        Command Line:

            --no-lane-splitting (default off)

        Sample sheet:

            NoLaneSplitting, true or false (default false)

        [ref]: https://knowledge.illumina.com/software/general/software-general-reference_material-list/000003710
        '''
        if not isinstance(value, bool) and not value == 0 and not value == 1:
            raise ValueError('NoLaneSplitting must be a boolean')
        if value or value.casefold() == 'true':
            self.data['NoLaneSplitting'] = 'true'
        else:
            self.data['NoLaneSplitting'] = 'false'


class TableSection:
    def __init__(self, data, name=None):
        '''
        data : pandas.DataFrame
        '''
        if not data.index.is_unique:
            raise ValueError('Index must be unique')
        self._data = data
        self._name = name

    def __repr__(self):
        return (
            f'{self.name}(\n'
            f'{self.data.head().to_string()}\n'
            '...\n'
            f'{self.data.shape[0]} rows, {self.data.shape[1]} columns\n'
            ')\n'
        )

    def __getitem__(self, *args, **kwargs):
        return self.data.__getitem__(*args, **kwargs)

    # def loc(self, *args, **kwargs):
    #     return self.data.loc(*args, **kwargs)
    # ^Nope, doesn't support assignment.

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        if not isinstance(value, pd.DataFrame):
            raise ValueError('Data must be a pandas DataFrame')
        if not value.index.is_unique:
            raise ValueError('Index must be unique')
        self._data = value

    @property
    def name(self):
        return self._name

    def to_csv(self, *args, file=None, **kwargs):
        text = f'[{self.name}]\n'
        kwargs.setdefault('index', None)
        text += self.data.to_csv(None, *args, **kwargs)
        text += '\n'
        if file is None:
            return text
        else:
            print(text, file=file)


class BaseSampleSheet:
    def __init__(self, path=None, content=None):
        self.path = path      # Use setter.
        self._content = content
        # Sections
        self._header = None   # Standard for all subclasses.
        self._reads = None    # Standard for all subclasses.
        # Extras
        self._info = None
        self._sample_project = None
        self._is_split_lane = None
        self._format = None
        # Data sections
        self._index1_col = None
        self._index2_col = None

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.path}")'

    @property
    def FileFormatVersion(self):
        '''
        The `FileFormatVersion` key is included in the v2 sample sheet but not
        v1. Therefore, if the key is not present, default to `1`.
        '''
        return int(self.Header.data.get('FileFormatVersion', 1))

    version = FileFormatVersion

    @property
    def format(self):
        return self._format

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, str):
            value = pathlib.Path(value)
        self._path = value

    @property
    def realpath(self):
        return self.path.resolve()

    @property
    def text(self):
        return self.path.read_text()

    @property
    def content(self):
        '''
        Helper for the other properties. Read the data and parse it into
        sections, but don't actually parse the sections yet.

        Returns
        -------
        dict
            A dictionary of sections, where each section is a list of lines.
        '''
        return self._content

    @property
    def sections(self):
        return []

    @property
    def Header(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._header:
            name = 'Header'
            self._header = parse_dict_section(self.content[name], name=name)
        return self._header

    @property
    def sample_project(self):
        '''
        [TODO] Rename: just use `projects` instead.
        '''
        if self._sample_project is None:
            self._sample_project = sorted(set(self.Data.data.get('Sample_Project', [])))
        return self._sample_project

    projects = sample_project

    @property
    def is_split_lane(self):
        if self._is_split_lane is None:
            self._is_split_lane = len(self.projects) > 1
        return self._is_split_lane

    @property
    def info(self):
        warnings.warn(
            (
                f'This object ("{self.__class__.__name__}")'
                ' has an `info` property, but it is not implemented.'
                ' Please implement a custom `info` property in your subclass.'
            ),
            UserWarning
        )

    def _verify_index1_col(self):
        return self._index1_col in self.Data.data.columns

    def _verify_index2_col(self):
        return self._index2_col in self.Data.data.columns

    def duplicate_indexes(self, use_lane=True, sort=True):
        '''
        Check for duplicate indexes in the sample sheet data.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the rows with duplicate indexes. The
            DataFrame is sorted by the lane, index1, and index2 columns.
        '''
        dupes = get_duplicate_indexes(
            self.Data.data,
            self._index1_col,
            self._index2_col,
            use_lane=use_lane,
            sort=sort
        )
        return dupes

    def hamming_distances(self, use_lane=True):
        '''
        Compute hamming distances between all pairs of indexes, i.e., comparing
        every single index from both index1 and index2 to every other index
        (NOT just comparing index1 to index2).

        [TODO] Also, consider that this method should calculate distances
        between *sample* indexes, i.e., the object that it operates on should
        be a *sample* instead of extracting indexes from the sample sheet data.
        '''
        # --- 8< ---
        # Calculate the hamming distances between ALL pairs of indexes, i.e.,
        # do NOT use a 'set' which will silently drop duplicate indexes. If the
        # sample sheet has duplicate indexes, then the hamming distance will be
        # 0 for those pairs.
        # if self._verify_index2_col():
        #     # ~~indexes = set(self.Data.data[self._index1_col]) | set(self.Data.data[self._index2_col])~~
        #     indexes = self.Data.data[self._index1_col].to_list() + self.Data.data[self._index2_col].to_list()
        # else:
        #     # ~~indexes = set(self.Data.data[self._index1_col])~~
        #     indexes = self.Data.data[self._index1_col].to_list()
        # U = [Seq(u) for u in sorted(indexes)]
        # --- >8 ---
        # [TODO] USE LANE !!!! This will calculate distances between all index
        # pairs, even for samples in DIFFERENT lanes. But the indexes only need
        # to be unique WITHIN each lane.
        if self._verify_index2_col():
            index_pairs = self.Data.data[self._index1_col] + '+' + self.Data.data[self._index2_col]
        else:
            index_pairs = self.Data.data[self._index1_col]
        U = [Seq(u) for u in sorted(index_pairs)]
        d = pairwise_hamming_distance(U)
        return sorted(d, key=lambda x: x['hamming'])

    def filter_sample_indexes(self, indexes, which='both', as_mask=False):
        '''
        Filter the sample sheet data by indexes, return the samples that match the given `indexes`.

        Note that some V2 sample sheets have only one `Index` column and no `Index2` column.
        '''
        if which == 'both':
            if self._verify_index2_col():
                mask_index1 = self.Data.data[self._index1_col].isin(indexes)
                mask_index2 = self.Data.data[self._index2_col].isin(indexes)
                mask = mask_index1 | mask_index2
            else:
                logger.warning('`index2` column not found, filtering only by `index1`.')
                mask = self.Data.data[self._index1_col].isin(indexes)
        elif which == 'index1':
            mask = self.Data.data[self._index1_col].isin(indexes)
        elif which == 'index2':
            if self._verify_index2_col():
                mask = self.Data.data[self._index2_col].isin(indexes)
            else:
                raise ValueError('`index2` column not found, cannot filter by `index2`.')
        else:
            raise ValueError(f'`which` must be "both", "index1", or "index2", you gave "{which}"')

        if as_mask:
            return mask
        else:
            return self.Data.data.loc[mask].copy()

    def merge_duplicate_indexes(self, use_lane=True, drop=True):
        if use_lane and 'Lane' in self.Data.data.columns:
            groupby_cols = ['Lane', self._index1_col, self._index2_col]
        else:
            groupby_cols = [self._index1_col, self._index2_col]
        is_dupe = self.Data.data.duplicated(subset=groupby_cols, keep=False)
        dupes = self.Data.data.loc[is_dupe]

        records = []
        for name, grp in dupes.groupby(groupby_cols):
            # Get the unique group values from the 'name'.
            if len(name) == 3:
                lane, index1, index2 = name
            elif len(name) == 2:
                index1, index2 = name
                lane = None
            else:
                raise ValueError(f'Groupby should have 2 or 3 items: `([lane,] index1, index2)`')
            rec = {}
            for col in grp.columns:
                if col == 'Lane' and lane:
                    # [TODO] Duplicate indexes **across lanes** (vs within lanes) ???
                    rec[col] = lane
                elif col == 'Sample_ID':
                    rec[col] = f'DUPLICATE_INDEX_{index1}_{index2}'
                elif col == 'Sample_Name':
                    rec[col] = f'DUPLICATE_INDEX_{index1}_{index2}'
                elif col == 'Sample_Project':
                    rec[col] = 'DUPLICATE_INDEXES'
                elif col in ['index', 'Index', 'I7_Index_ID']:
                    rec[col] = index1
                elif col in ['index2', 'Index2', 'I5_Index_ID']:
                    rec[col] = index2
            records.append(rec)

        if drop:
            orig = self.Data.data.loc[~is_dupe].copy()
        else:
            orig = self.Data.data.copy()
        
        if not records:
            logger.warning('The `merge_duplicate_indexes` function found 0 duplicate indexes.')
            new = orig
        else:
            new = pd.concat(
                [orig, pd.DataFrame(records)],
                ignore_index=True
            )

        # print(new)

        self.Data.data = new
        return None

    def to_csv(self, *args, file=None, **kwargs):
        text = ''
        for section in self.sections:
            text += section.to_csv(*args, file=file, **kwargs)
        if file is None:
            return text
        else:
            print(text, file=file)


class SampleSheetv1(BaseSampleSheet):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault(
            'content',
            {'Header': [], 'Reads': [], 'Settings': [], 'Data': []}
        )
        super().__init__(*args, **kwargs)
        self._format = 'v1'
        self._settings = None
        self._data = None
        self._index1_col = 'index'
        self._index2_col = 'index2'

    @property
    def sections(self):
        return [
            self.Header,
            self.Reads,
            self.Settings,
            self.Data,
        ]

    @property
    def Reads(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._reads:
            name = 'Reads'
            self._reads = parse_list_section(self.content[name], name=name)
        return self._reads

    @property
    def Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._settings:
            name = 'Settings'
            self._settings = parse_dict_section(
                self.content[name],
                name=name,
                cls=SettingsSection
            )
        return self._settings

    @property
    def Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._data:
            name = 'Data'
            self._data = parse_table_section(self.content[name], name=name)
        return self._data
    
    @property
    def info(self):
        self._info = { **self.Header.data }
        self._info['projects'] = self.projects
        self._info['is_split_lane'] = self.is_split_lane
        self._info['samplesheet_version'] = self.version
        self._info['samplesheet_path'] = str(self.path.resolve())
        self._info['samplesheet_filename'] = self.path.name
        return self._info


class SampleSheetv2(BaseSampleSheet):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault(
            'content',
            {'Header': [], 'Reads': [], 'BCLConvert_Settings': [], 'BCLConvert_Data': [], 'Cloud_Settings': [], 'Cloud_Data': []}
        )
        super().__init__(*args, **kwargs)
        self._format = 'v2'
        self._bclconvert_settings = None
        self._bclconvert_data = None
        self._index1_col = 'Index'
        self._index2_col = 'Index2'
        self._cloud_settings = None
        self._cloud_data = None

    @property
    def sections(self):
        return [
            self.Header,
            self.Reads,
            self.BCLConvert_Settings,
            self.BCLConvert_Data,
            self.Cloud_Settings,
            self.Cloud_Data,
        ]

    @property
    def Reads(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._reads:
            name = 'Reads'
            self._reads = parse_dict_section(self.content[name], name=name)
        return self._reads

    @property
    def BCLConvert_Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._bclconvert_settings:
            name = 'BCLConvert_Settings'
            self._bclconvert_settings = parse_dict_section(
                self.content[name],
                name=name,
                cls=BCLConvertSettingsSection
            )
        return self._bclconvert_settings

    Settings = BCLConvert_Settings

    @property
    def Cloud_Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._cloud_settings:
            name = 'Cloud_Settings'
            self._cloud_settings = parse_dict_section(self.content[name], name=name)
        return self._cloud_settings

    @property
    def BCLConvert_Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._bclconvert_data:
            name = 'BCLConvert_Data'
            self._bclconvert_data = parse_table_section(self.content[name], name=name)
        return self._bclconvert_data

    Data = BCLConvert_Data
    
    @property
    def Cloud_Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._cloud_data:
            name = 'Cloud_Data'
            self._cloud_data = parse_table_section(self.content[name], name=name)
        return self._cloud_data
    
    @property
    def sample_project(self):
        '''
        [TODO] Rename: just use `projects` instead.
        '''
        if self._sample_project is None:
            if 'Sample_Project' in self.BCLConvert_Data.data.columns:
                self._sample_project = sorted(set(self.BCLConvert_Data.data['Sample_Project']))
            elif 'ProjectName' in self.Cloud_Data.data.columns:
                l = self.Cloud_Data.data['ProjectName'].tolist()
                self._sample_project = sorted(set(l))
        return self._sample_project

    projects = sample_project

    @property
    def info(self):
        self._info = { **self.Header.data }
        self._info['projects'] = self.projects
        self._info['is_split_lane'] = self.is_split_lane
        self._info['samplesheet_version'] = self.version
        self._info['samplesheet_path'] = str(self.path.resolve())
        self._info['samplesheet_filename'] = self.path.name
        return self._info

    def projectname_to_sampleproject(self):
        '''
        Convert the `ProjectName` column in the `Cloud_Data` section to
        `Sample_Project` in the `BCLConvert_Data` section.

        Returns
        -------
        None
        '''
        mapper = get_sample_project(self.Cloud_Data.data, project_col='ProjectName')
        # This should set the data in place.
        set_sample_project(self.BCLConvert_Data.data, mapper)
        return None


# [TODO] Move to illumina/sample_sheet.py
class Barcode:
    _allowed_chars = set('actgn+')
    def __init__(self, sequence=None, i7=None, i5=None, sep='-'):
        self.sep = sep
        if sequence and not i7 and not i5:
            if '+' in sequence:
                self.sep = '+'
            try:
                i7, i5 = sequence.split(self.sep)
            except ValueError as e:
                raise ValueError(f'Invalid sequence: {sequence}')
        elif i7 and i5 and not sequence:
            pass
        else:
            raise ValueError('You must provide "sequence OR ( i7 AND i5 )" ')
        self.i7 = i7
        self.i5 = i5
        self._full = Seq(f'{i7}{self.sep}{i5}')

    def __str__(self):
        return f'{self.i7}{self.sep}{self.i5}'

    def __repr__(self):
        return f'Barcode({self.__str__()})'

    @property
    def i7(self):
        '''
        Index 1 (left-hand side)
        '''
        return self._i7

    @i7.setter
    def i7(self, value):
        '''  
        The Index 1 (i7) index adapter sequence.
        https://support-docs.ilnplumina.com/SW/BCL_Convert/Content/SW/BCLConvert/SampleSheets_swBCL.htm
        '''
        if not set(str(value).casefold()) <= self._allowed_chars:
            raise ValueError(f'Invalid sequence: {value}')
        self._i7 = Seq(value)

    @property
    def i5(self):
        '''
        Index 2 (right-hand side)
        '''
        return self._i5

    @i5.setter
    def i5(self, value):
        '''
        The Index 2 (i5) Index adapter sequence.
        https://support-docs.illumina.com/SW/BCL_Convert/Content/SW/BCLConvert/SampleSheets_swBCL.htm
        '''
        if not set(str(value).casefold()) <= self._allowed_chars:
            raise ValueError(f'Invalid sequence: {value}')
        self._i5 = Seq(value)


    def rc1(self):
        '''
        Reverse complement method 1: i7 only.
        '''
        return Barcode(i7=self.i7.reverse_complement(), i5=self.i5)

    def rc2(self):
        '''
        Reverse complement method 2: i5 only.
        '''
        return Barcode(i7=self.i7, i5=self.i5.reverse_complement())

    def rc3(self):
        '''
        Reverse complement method 3: i7 and i5 independently ("both").
        '''
        return Barcode(i7=self.i7.reverse_complement(), i5=self.i5.reverse_complement())

    def rc4(self):
        '''
        Reverse complement method 4: "full" sequence (disregard i7 and i5).
        '''
        return Barcode(self._full.reverse_complement())

    def reverse_complement(self, method=1):
        '''
        There are 4 ways that you can reverse complement the barcode:
        1. i7 only
        2. i5 only
        3. i7 and i5 independently ("both")
        4. i7 and i5 together ("full", ie, treat it as 1 sequence)
        '''
        if method == 'i7' or method == 1:
            return self.rc1()
        if method == 'i5' or method == 2:
            return self.rc2()
        elif method == 'both' or method == 3:
            return self.rc3()
        elif method == 'full' or method == 4:
            return self.rc4()
        elif method.casefold() == 'none':
            # Dummy. Just return the original sequence.
            # Should this issue a warning?
            return self
        else:
            raise ValueError(f'Invalid method: {method}')


# [TODO] Move to illumina/helpers.py
def compare_barcodes(b1, b2, verbose=False):
    '''
    Check if any reverse complemented form of "b2" matches "b1".
    '''
    matches = {}
    for method in ['None', 'i7', 'i5', 'both', 'full']:
        rc = b2.reverse_complement(method=method)
        if str(b1) == str(rc):
            if verbose:
                print(f'Original : {b1} .. {b2}')
                print(f'Match    : {b1} <- {rc}')
            matches[method] = rc
    return matches


class IlluminaSequencingData(base.RawData):
    # getter and setter methods:
    # https://www.geeksforgeeks.org/getter-and-setter-in-python/
    def __init__(self, rundir, runid=None, instrument=None, path_to_samplesheet=None, **kwargs):
        if 'path' in kwargs:
            warnings.warn('The `path` kwarg is ignored. Use `rundir` instead.')
        self._rundir = pathlib.Path(rundir)

        # if runid is None:
        #     # self._runid = regex_runid.search(str(rundir)).group(0)
        #     self._runid = utils.get_runid(self._rundir)
        # else:
        #     self._runid = runid

        self._runid = self._rundir.name

        if instrument is None:
            for id_, name in _instrument_id.items():
                if id_ in self._runid:
                    self._instrument = name
                    break
        elif instrument not in _instruments:
            raise ValueError(f'`instrument` must be one of {_instruments!r} or None')
        else:
            self._instrument = 'UNKNOWN'

        # self._path_to_samplesheet = path_to_samplesheet
        if path_to_samplesheet is None:
            self._path_to_samplesheet = self._rundir / 'SampleSheet.csv'
        else:
            if isinstance(path_to_samplesheet, str):
                path_to_samplesheet = pathlib.Path(path_to_samplesheet)
            self._path_to_samplesheet = path_to_samplesheet
        self._samplesheet = None
        self._info = None
        self._samples = None
        self._sample_project = None
        self._is_split_lane = None
        super().__init__(path=self._rundir)

    @property
    def rundir(self):
        return self._rundir

    @property
    def runid(self):
        return self._runid
    
    @property
    def instrument(self):
        return self._instrument

    @property
    def path_to_samplesheet(self):
        return self._path_to_samplesheet

    @path_to_samplesheet.setter
    def path_to_samplesheet(self, path):
        self._path_to_samplesheet = pathlib.Path(path)

    @property
    def samplesheet(self):
        if not self._samplesheet:
            self._samplesheet = read_sample_sheet(self.path_to_samplesheet, version='infer')
        return self._samplesheet

    # @samplesheet.setter
    # def samplesheet(self, value):
    #     if value is None:
    #         raise ValueError(f'A path is required but you gave "{value}"')
    #     self._samplesheet = value

    @property
    def name(self):
        if 'Experiment Name' in self.samplesheet.Header.data:
            return self.samplesheet.Header.data['Experiment Name']
        elif 'RunName' in self.samplesheet.Header.data:
            return self.samplesheet.Header.data['RunName']

    @property
    def projects(self):
        return self.samplesheet.projects

    def find_samplesheet(self):
        # found_items = ss.find_samplesheet(self.rundir)
        # if len(found_items) == 0:
        #     raise ValueError(f'No sample sheet found: {self.path}')
        # else:
        #     self.path_to_samplesheet = found_items[0]
        #     if len(found_items) > 1:
        #         print(f'[WARNING] Found {len(found_items)} possible sample sheets:', file=sys.stderr)
        #         for item in found_items:
        #             print(item, file=sys.stderr)
        pass

    # def ls(self):
    #     for path in sorted(self.path.iterdir()):
    #         print(path)

    @property
    def info(self):
        if self.path_to_samplesheet.exists():
            self._info = { **self.samplesheet.info }
        else:
            self._info = {
                'projects': [],
                'samplesheet_path': None,
                'samplesheet_filename': None
            }
        self._info['runid'] = self.runid
        self._info['instrument'] = self.instrument
        self._info['rundir'] = str(self.rundir.resolve())
        return self._info

# END

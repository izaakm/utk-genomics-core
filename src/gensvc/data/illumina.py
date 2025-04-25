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
import os
import pandas as pd
import pathlib
import re
import sys
import warnings
import logging

from io import StringIO
from datetime import datetime
from gensvc.misc import utils
from gensvc.data import base


_instrument_id ={
    'FS10003266': 'iSeq',
    'M04398': 'MiSeq',
    'VL00838': 'NextSeq',
    'A01770': 'NovaSeq'
}

_instruments = sorted(_instrument_id.values())

regex_runid = re.compile(r'[^\/]*\d{6,8}[^\/]*')
section = re.compile(r'^\[\s*(\w+)\s*]')

logger = logging.getLogger(__name__)


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
            sec = section.match(line)
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


def parse_dict_section(lines, name=None):
    data = {}
    for line in lines:
        if not line:
            continue
        key, val, *_ = line.split(',')
        data[key] = val
    return DictSection(data, name=name)


def parse_table_section(lines, index_col='Sample_ID', name=None):
    # data = pd.read_csv(io.StringIO(data.strip()))
    columns = lines.pop(0).split(',')
    rows = [line.split(',') for line in lines]
    data = pd.DataFrame(rows, columns=columns)
    data = data.dropna(axis=0, how='all')
    # data.index = data[index_col]
    # data.index.name = None
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


def samples_to_dataframe(samplesheet):
    return pd.DataFrame([s.to_json() for s in samplesheet.samples])


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
    def __init__(self, path, content=None):
        self.path = path
        if content is None:
            self._content = parse_sample_sheet(path)
        elif isinstance(content, dict):
            self._content = content
        else:
            raise ValueError(f'`content` must be a dict or None, you gave "{type(content)}"')
        self._header = None
        self._reads = None
        self._sample_project = None
        self._is_split_lane = None
        self._sections = []

    def __repr__(self):
        return f'{self.__class__.__name__}("{self._path}")'

    @property
    def FileFormatVersion(self):
        '''
        The `FileFormatVersion` key is included in the v2 sample sheet but not
        v1. Therefore, if the key is not present, default to `1`.
        '''
        return int(self.Header.get('FileFormatVersion', 1))

    version = FileFormatVersion

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = pathlib.Path(value)

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
        '''
        return self._content

    @property
    def sections(self):
        return [key for key in self.content.keys()]

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
    def Reads(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._reads:
            name = 'Reads'
            self._reads = parse_dict_section(self.content[name], name=name)
        return self._reads

    @property
    def sample_project(self):
        if self._sample_project is None:
            self._sample_project = sorted(set(self.samples['Sample_Project']))
        return self._sample_project

    projects = sample_project

    @property
    def is_split_lane(self):
        if self._is_split_lane is None:
            self._is_split_lane = len(self.projects) > 1
        return self._is_split_lane

    def to_csv(self, *args, file=None, **kwargs):
        text = ''
        for section in self._sections:
            text += section.to_csv(*args, file=None, **kwargs)
        if file is None:
            return text
        else:
            print(text, file=file)


class SampleSheetv1(BaseSampleSheet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sections = [
            self.Header,
            self.Reads,
            self.Settings,
            self.Data,
        ]

    @property
    def Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._settings:
            name = 'Settings'
            self._settings = parse_dict_section(self.content[name], name=name)
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
    
    samples = Data


class SampleSheetv2(BaseSampleSheet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bclconvert_settings = None
        self._bclconvert_data = None
        self._cloud_settings = None
        self._cloud_data = None
        self._sections = [
            self.Header,
            self.Reads,
            self.BCLConvert_Settings,
            self.BCLConvert_Data,
            self.Cloud_Settings,
            self.Cloud_Data,
        ]

    @property
    def BCLConvert_Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._bclconvert_settings:
            name = 'BCLConvert_Settings'
            self._bclconvert_settings = parse_dict_section(self.content[name], name=name)
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
    
    samples = Data

    @property
    def sample_project(self):
        if self._sample_project is None:
            if 'Sample_Project' in self.BCLConvert_Data.data.columns:
                self._sample_project = sorted(set(self.BCLConvert_Data.data['Sample_Project']))
            elif 'ProjectName' in self.Cloud_Data.data.columns:
                l = self.Cloud_Data.data['ProjectName'].tolist()
                self._sample_project = sorted(set(l))
        return self._sample_project

    projects = sample_project


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

    def ls(self):
        for path in sorted(self.path.iterdir()):
            print(path)


# END

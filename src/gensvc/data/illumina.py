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


def csv_read(content):
    '''
    This is the preferred function for reading CSV data. This function and
    'csv_split' are similar in performance on small data sets, but 'csv_split'
    is really just included for comparison.
    '''
    if isinstance(content, list):
        content = '\n'.join(content)
    f = StringIO(content)
    reader = csv.reader(f)
    for row in reader:
        yield row


def csv_split(content):
    '''
    This function and 'csv_read' are similar in performance on small data sets,
    but 'csv_read' should be preferred because it uses 'csv.reader' from the
    standard lib, and is therefore expected to be more reliable.
    '''
    if isinstance(content, str):
        content = content.split('\n')
    for line in content:
        yield line.split(',')


def parse_header(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Header' section of an Illumina Sample Sheet.
    '''
    data = {}
    reader = csv_reader(list_of_lines)
    for row in reader:
        vals = [i for i in row if i]
        if not vals:
            continue
        elif len(vals) != 2:
            raise ValueError(f'Too many values: {vals!r}')
        else:
            data[vals[0]] = vals[1]
    return data


def parse_reads(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Reads' section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    data = []
    for row in reader:
        tmp = [int(i) for i in row if i]
        if tmp:
            data.append(tmp)
    return data


def parse_settings(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Settings' section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    data = []
    for row in reader:
        tmp = [i for i in row if i]
        if tmp:
            data.append(tmp)
    return data


def parse_data(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Data' (samples) section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    header = next(reader)
    data = []
    for row in reader:
        data.append(dict(zip(header, row)))
    return data


def get_content(path):
    # header = re.compile(r'^\[\s*Header\s*]')
    section = re.compile(r'^\[\s*(\w+)\s*]')
    content = dict()
    lines = []
    key = None

    with open(path) as f:
        for line in f:
            sec = section.match(line)
            if sec:
                # New section
                # print(sec.group(0), sec.group(1))
                if key and lines:
                    content[key] = lines

                key = sec.group(1)
                lines = []
            else:
                lines.append(line.strip())
        else:
            if key and lines:
                content[key] = lines

    return content


def read_samplesheet(path, version='infer'):
    content = get_content(path)

    if version == 'raw':
        return content

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


def looks_like_samplesheet(path):
    # print('Checking path ...')
    if not isinstance(path, pathlib.Path):
        try:
            path = pathlib.Path(path)
        except:
            # print('Cannot convert to path.')
            return False

    if not path.is_file():
        # print('Not a file.')
        return False

    try:
        sample_sheet = read_samplesheet(path, version='raw')
    except:
        # print('Cannot read file.')
        return False

    if sample_sheet.get('Header') and sample_sheet.get('Reads'):
        # print('FOUND SAMPLE SHEET!!!')
        return True
    else:
        # print('Missing "Header" or "Reads"')
        return False


def samples_to_dataframe(samplesheet):
    return pd.DataFrame([s.to_json() for s in samplesheet.samples])


class BaseSampleSheet:
    def __init__(self, path, content=None):
        self._path = path
        self._content = content
        self._header = None
        self._reads = None
        self._settings = None
        self._data = None
        self._sample_project = None
        self._is_split_lane = None

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
        return self._path.resolve()

    @property
    def text(self):
        return self.path.read_text()

    @property
    def content(self):
        '''
        Helper for the other properties. Read the data and parse it into
        sections, but don't actually parse the sections yet.
        '''
        if not self._content:
            self._content = read_samplesheet(self.path)
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
            self._header = parse_header(self.content.get('Header', []))
        return self._header

    @property
    def Reads(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._reads:
            self._reads = parse_reads(self.content.get('Reads', []))
        return self._reads

    @property
    def Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._settings:
            self._settings = parse_settings(self.content.get('Settings', []))
        return self._settings

    @property
    def Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._data:
            self._data = parse_data(self.content.get('Data', []))
        return self._data
    
    samples = Data

    @property
    def sample_project(self):
        if self._sample_project is None:
            self._sample_project = sorted(set(self.samples['Sample_Project']))
        return self._sample_project

    projects = sample_project

    @property
    def is_split_lane(self):
        if self._is_split_lane is None:
            self._is_split_lane = len(self.sample_project) > 1
        return self._is_split_lane


class SampleSheetv1(BaseSampleSheet):
    pass


class SampleSheetv2(BaseSampleSheet):

    @property
    def BCLConvert_Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._settings:
            self._settings = parse_settings(self.content.get('BCLConvert_Settings', []))
        return self._settings

    Settings = BCLConvert_Settings

    @property
    def Cloud_Settings(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._settings:
            self._settings = parse_settings(self.content.get('Cloud_Settings', []))
        return self._settings

    @property
    def BCLConvert_Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._data:
            self._data = parse_data(self.content.get('BCLConvert_Data', []))
        return self._data

    Data = BCLConvert_Data
    
    @property
    def Cloud_Data(self):
        '''
        [TODO] Consider moving fxn into class as method.
        '''
        if not self._data:
            self._data = parse_data(self.content.get('Cloud_Data', []))
        return self._data
    
    samples = Data


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
            self.samplesheet = read_samplesheet(self.path_to_samplesheet, version='infer')
        return self._samplesheet

    @samplesheet.setter
    def samplesheet(self, value):
        if value is None:
            raise ValueError(f'A path is required but you gave "{value}"')
        self._samplesheet = value

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

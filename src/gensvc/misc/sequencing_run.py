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


import pathlib
import os
import hashlib
import pandas as pd
import re
import sys
import warnings

# from sample_sheet import SampleSheet
from datetime import datetime
# from gensvc.misc import utils, samplesheet
from gensvc.misc import utils
# from gensvc import misc
from gensvc.misc import samplesheet as ss

# regex_runid = re.compile('[^\/]*\d{6}[^\/]*')

_get_path_or_none = lambda name: pathlib.Path(os.getenv(name)) if name in os.environ else None

_instrument_id ={
    'FS10003266': 'iSeq',
    'M04398': 'MiSeq',
    'VL00838': 'NextSeq',
    'A01770': 'NovaSeq'
}

_instruments = sorted(_instrument_id.values())

if 'GENSVC_DIR' in os.environ:
    GENSVC_DIR = pathlib.Path(os.getenv('GENSVC_DIR'))
    # GENSVC_NOVASEQDATA = pathlib.Path(os.getenv('GENSVC_NOVASEQDATA')) or GENSVC_DIR / 'NovaSeqRuns'
    GENSVC_NOVASEQDATA = _get_path_or_none('GENSVC_NOVASEQDATA') or GENSVC_DIR / 'NovaSeqRuns'
    # GENSVC_MISEQDATA = pathlib.Path(os.getenv('GENSVC_NOVASEQDATA')) or GENSVC_DIR / 'MiSeqRuns'
    GENSVC_MISEQDATA = _get_path_or_none('GENSVC_MISEQDATA') or GENSVC_DIR / 'MiSeqRuns'
    # GENSVC_PROCDATA = pathlib.Path(os.getenv('GENSVC_PROCDATA')) or GENSVC_DIR / 'processed'
    GENSVC_PROCDATA = _get_path_or_none('GENSVC_PROCDATA') or GENSVC_DIR / 'processed'
else:
    GENSVC_DIR = None
    GENSVC_NOVASEQDATA = None
    GENSVC_MISEQDATA = None
    GENSVC_PROCDATA = None


def find(runid, datadir):
    if not datadir.is_dir():
        raise ValueError('not a directory: "{datadir}"')
    for item in datadir.glob('*'):
        if item.name == runid:
            return item


def find_rundir(runid, miseqdir=GENSVC_MISEQDATA, novaseqdir=GENSVC_NOVASEQDATA):
    rundir = find(runid, miseqdir) or find(runid, novaseqdir)
    return rundir


def find_procdir(runid, procdir=GENSVC_PROCDATA):
    rundir = find(runid, procdir)
    if rundir and rundir.is_dir():
        contents = sorted([item for item in rundir.glob('*') if item.is_dir()])
        return contents[-1]
    else:
        return None


def new_procdir(runid, procdir=GENSVC_PROCDATA):
    if not procdir or not procdir.is_dir():
        raise ValueError(f'not a directory: "{procdir}"')
    return procdir / runid / datetime.now().strftime('%Y%m%dT%H%M%S')


def md5sum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
    

def samples_to_dataframe(samplesheet):
    return pd.DataFrame([s.to_json() for s in samplesheet.samples])


class Datadir():
    def __init__(self, path):
        self._path = pathlib.Path(path)

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.path}")'

    @property
    def path(self):
        return self._path

    @property
    def realpath(self):
        return self._path.resolve()

    @property
    def info(self):
        return self.__dict__


class RawData(Datadir):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class IlluminaSequencingData(RawData):
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
            self.samplesheet = ss.SampleSheet(self.path_to_samplesheet)
        return self._samplesheet

    @samplesheet.setter
    def samplesheet(self, value):
        if value is None:
            raise ValueError(f'A path is required but you gave "{value}"')
        self._samplesheet = value

    def find_samplesheet(self):
        found_items = ss.find_samplesheet(self.rundir)
        if len(found_items) == 0:
            raise ValueError(f'No sample sheet found: {self.path}')
        else:
            self.path_to_samplesheet = found_items[0]
            if len(found_items) > 1:
                print(f'[WARNING] Found {len(found_items)} possible sample sheets:', file=sys.stderr)
                for item in found_items:
                    print(item, file=sys.stderr)

    @property
    def samples(self):
        if self.samplesheet is None:
            raise ValueError('samplesheet is not set')
        elif self._samples is None:
            self._samples = samples_to_dataframe(self.samplesheet)
        return self._samples

    @property
    def sample_project(self):
        print('[WARNING] the "sample_project" property is DEPRECATED')
        if self._sample_project is None:
            self._sample_project = sorted(set(self.samples['Sample_Project']))
        return self._sample_project

    @property
    def projects(self):
        return self.samplesheet.projects

    @property
    def is_split_lane(self):
        if self._is_split_lane is None:
            self._is_split_lane = len(self.sample_project) > 1
        return self._is_split_lane
    
    def ls(self):
        for path in sorted(self.path.iterdir()):
            print(path)


class ProcessedData(Datadir):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


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

class TransferData(ProcessedData):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

# END

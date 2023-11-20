import pathlib
import os
import hashlib
import pandas as pd
import re
import warnings

from sample_sheet import SampleSheet
from datetime import datetime
from gensvc.misc import utils, samplesheet

# regex_runid = re.compile('[^\/]*\d{6}[^\/]*')

_get_path_or_none = lambda name: pathlib.Path(os.getenv(name)) if name in os.environ else None

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
    def __init__(self, path=None):
        self._path = pathlib.Path(path)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self._path}>'

    def _get_path(self):
        return self._path

    path = property(_get_path)

    def _get_realpath(self):
        return self._path.resolve()

    realpath = property(_get_realpath)

class RawData(Datadir):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class IlluminaSequencingData(RawData):
    # getter and setter methods:
    # https://www.geeksforgeeks.org/getter-and-setter-in-python/
    def __init__(self, rundir, runid=None, instrument='unknown', path_to_samplesheet=None, **kwargs):
        # self._path = pathlib.Path(rundir)
        if 'path' in kwargs:
            warnings.warn('The `path` kwarg is ignored. Use `rundir` instead.')
        self._rundir = pathlib.Path(rundir)
        kwargs['path'] = self._rundir

        if runid is None:
            # self._runid = regex_runid.search(str(rundir)).group(0)
            self._runid = utils.get_runid(self._rundir)
        else:
            self._runid = runid

        if instrument not in ['unknown', 'MiSeq', 'NovaSeq']:
            raise ValueError('`instrument` must be one of "MiSeq", "NovaSeq", or None')
        self._instrument = instrument

        if path_to_samplesheet is None:
            self._path_to_samplesheet, self._all_samplesheets = samplesheet.find_samplesheet(self._rundir)
        else:
            self._path_to_samplesheet = path_to_samplesheet
            self._all_samplesheets = None

        self._samplesheet = None
        self._info = None
        self._samples = None
        self._sample_project = None
        self._is_split_lane = None
        super().__init__(**kwargs)

    def __repr__(self):
        return (
            f'<{self.__class__.__name__}: {self.instrument}, {self.runid}>'
        )

    def _get_rundir(self):
        return self._rundir

    rundir = property(_get_rundir)

    def _get_realpath(self):
        return self._path.resolve()

    realpath = property(_get_realpath)

    def _get_runid(self):
        return self._runid

    runid = property(_get_runid)
    
    def _get_instrument(self):
        if self._instrument == 'unknown':
            if self.samplesheet:
                self._instrument = self.samplesheet.Header.get('Instrument Type', 'unknown')
        return self._instrument

    instrument = property(_get_instrument)

    def _get_path_to_samplesheet(self):
        return self._path_to_samplesheet

    def _set_path_to_samplesheet(self, path_to_samplesheet):
        self._path_to_samplesheet = pathlib.Path(path_to_samplesheet)

    def _del_path_to_samplesheet(self):
        self._path_to_samplesheet = None

    path_to_samplesheet = property(fget=_get_path_to_samplesheet, fset=_set_path_to_samplesheet, fdel=_del_path_to_samplesheet)

    def _get_samplesheet(self):
        if self._samplesheet is None:
            if self.path_to_samplesheet:
                self._samplesheet = SampleSheet(self.path_to_samplesheet)
        return self._samplesheet

    def _del_samplesheet(self):
        self._samplesheet = None

    samplesheet = property(fget=_get_samplesheet, fdel=_del_samplesheet)

    def _get_info(self):
        if self._info is None:
            self._info = dict()
            self._info['runid'] = self.runid
            self._info['rundir'] = self.rundir
            self._info['path_to_samplesheet'] = self._path_to_samplesheet
            if self.samplesheet:
                self._info.update(self.samplesheet.Header.copy())
                self._info['sample_project'] = self.sample_project
        return self._info

    def _del_info(self):
        self._info = None

    info = property(fget=_get_info, fdel=_del_info)

    def _get_samples(self):
        if self.samplesheet is None:
            raise ValueError('samplesheet is not set')
        elif self._samples is None:
            self._samples = samples_to_dataframe(self.samplesheet)
        return self._samples

    def _del_samples(self):
        self._samples = None

    samples = property(fget=_get_samples, fdel=_del_samples)

    def _get_sample_project(self):
        if self._sample_project is None:
            self._sample_project = sorted(set(self.samples['Sample_Project']))
        return self._sample_project

    def _del_sample_project(self):
        self._sample_project = None

    sample_project = property(fget=_get_sample_project, fdel=_del_sample_project)

    def _get_is_split_lane(self):
        if self._is_split_lane is None:
            self._is_split_lane = len(self.sample_project) > 1
        return self._is_split_lane

    def _del_is_split_lane(self):
        self._is_split_lane = None

    is_split_lane = property(fget=_get_is_split_lane, fdel=_del_is_split_lane)


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

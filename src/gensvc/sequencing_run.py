import pathlib
import os
import hashlib
import pandas as pd

from sample_sheet import SampleSheet

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
    for item in datadir.glob('*'):
        if item.name == runid:
            return item


def find_rundir(runid, miseqdir=GENSVC_MISEQDATA, novaseqdir=GENSVC_NOVASEQDATA):
    rundir = find(runid, miseqdir) or find(runid, novaseqdir)
    return rundir


def find_procdir(runid, procdir=GENSVC_PROCDATA):
    rundir = find(runid, procdir)
    contents = sorted([item for item in rundir.glob('*') if item.is_dir()])
    return contents[-1]


def md5sum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
    
def find_run_samplesheet(rundir, depth=1, verbose=False, as_dataframe=True, include_symlinks=False):
    # filelist = list(rundir.rglob('*.csv'))
    # for path in filelist:
    #     print(path)
    files = []
    for i, path in enumerate(rundir.glob('*.csv'), start=1):
        if not include_symlinks and path.is_symlink():
            continue
        try:
            SampleSheet(path)
            files.append(dict(
                path=path,
                md5=md5sum(path),
                mtime=path.stat().st_mtime
            ))
        except:
            pass
    if depth > 1:
        for i, path in enumerate(rundir.glob('*/*.csv'), start=i+1):
            if not include_symlinks and path.is_symlink():
                continue
            try:
                SampleSheet(path)
                files.append(dict(
                    path=path,
                    md5=md5sum(path),
                    mtime=path.stat().st_mtime
                ))
            except:
                pass
    if verbose:
        print(f'Scanned {i} files under "{rundir}".')
        print(f'Found {len(files)} sample sheets.')
    if as_dataframe:
        files = pd.DataFrame(files)
    return files

def samples_to_dataframe(samplesheet):
    return pd.DataFrame([s.to_json() for s in samplesheet.samples])

class SeqRun():
    # getter and setter methods:
    # https://www.geeksforgeeks.org/getter-and-setter-in-python/
    def __init__(self, runid=None, rundir=None, procdir=None, instrument='unknown', path_to_samplesheet=None):
        self._runid = runid
        self._rundir = rundir
        self._procdir = procdir
        self._instrument = instrument
        self._path_to_samplesheet = path_to_samplesheet
        self._samplesheet = None
        self._info = None
        self._samples = None
        self._sample_project = None
        self._is_split_lane = None

    def __repr__(self):
        return (
            f'<Sequencing run, {self.instrument}, {self.runid}'
        )

    def _get_rundir(self):
        return self._rundir

    def _set_rundir(self, rundir):
        self._rundir = rundir

    def _del_rundir(self):
        self._rundir = None

    rundir = property(fget=_get_rundir, fset=_set_rundir, fdel=_del_rundir)
    
    def _get_procdir(self):
        return self._procdir

    def _set_procdir(self, procdir):
        self._procdir = procdir

    def _del_procdir(self):
        self._procdir = None

    procdir = property(fget=_get_procdir, fset=_set_procdir, fdel=_del_procdir)

    def _get_instrument(self):
        return self._instrument

    def _set_instrument(self, instrument):
        self._instrument = instrument

    def _del_instrument(self):
        self._instrument = None

    instrument = property(fget=_get_instrument, fset=_set_instrument, fdel=_del_instrument)

    def _get_path_to_samplesheet(self):
        return self._path_to_samplesheet

    def _set_path_to_samplesheet(self, path_to_samplesheet):
        self._path_to_samplesheet = path_to_samplesheet

    def _del_path_to_samplesheet(self):
        self._path_to_samplesheet = None

    path_to_samplesheet = property(fget=_get_path_to_samplesheet, fset=_set_path_to_samplesheet, fdel=_del_path_to_samplesheet)

    def _get_samplesheet(self):
        return self._samplesheet

    def _set_samplesheet(self):
        if self.path_to_samplesheet is None:
            raise ValueError('path_to_samplesheet is not set')
        self._samplesheet = SampleSheet(self.path_to_samplesheet)

    def _del_samplesheet(self):
        self._samplesheet = None

    samplesheet = property(fget=_get_samplesheet, fset=_set_samplesheet, fdel=_del_samplesheet)

    def _get_info(self):
        return self._info

    def _set_info(self):
        if self.samplesheet is None:
            raise ValueError('samplesheet is not set')
        self._info = self._samplesheet.Header.copy()
        self._info['runid'] = self._runid
        self._info['rundir'] = self._rundir
        self._info['procdir'] = self._procdir
        self._info['path_to_samplesheet'] = self._path_to_samplesheet

    def _del_info(self):
        self._info = None

    info = property(fget=_get_info, fset=_set_info, fdel=_del_info)

    def _get_samples(self):
        return self._samples

    def _set_samples(self):
        if self.samplesheet is None:
            raise ValueError('samplesheet is not set')
        self._samples = samples_to_dataframe(self.samplesheet)

    def _del_samples(self):
        self._samples = None

    samples = property(fget=_get_samples, fset=_set_samples, fdel=_del_samples)

    def _get_sample_project(self):
        return self._sample_project

    def _set_sample_project(self):
        if self.samplesheet is None:
            raise ValueError('samplesheet is not set')
        self._sample_project = sorted(set(self.samples['Sample_Project']))

    def _del_sample_project(self):
        self._sample_project = None

    sample_project = property(fget=_get_sample_project, fset=_set_sample_project, fdel=_del_sample_project)

    def _get_is_split_lane(self):
        return self._is_split_lane

    def _set_is_split_lane(self):
        if not isinstance(self.sample_project, list):
            raise ValueError('sample_project should be type list')
        if not self.sample_project:
            raise ValueError('sample_project list should not be empty')
        self._is_split_lane = len(self.sample_project) > 1

    def _del_is_split_lane(self):
        self._is_split_lane = None

    is_split_lane = property(fget=_get_is_split_lane, fset=_set_is_split_lane, fdel=_del_is_split_lane)

    def find_datadirs(self):
        if self._runid is None:
            raise ValueError('runid not set')
        if self.rundir is None:
            self.rundir = find_rundir(self._runid)
        if self.procdir is None:
            self.procdir = find_procdir(self._runid)

    def find_samplesheet(self):
        # HOLD For now, the sample sheet should be passed expilititely.
        # Reconsider how to handle the situation when there are multiple sample sheets.
        pass

    def load_samplesheet(self, path_to_samplesheet=None):
        if path_to_samplesheet:
            self.path_to_samplesheet = path_to_samplesheet
        self._set_samplesheet()
        self._set_info()
        self._set_samples()
        self._set_sample_project()
        self._set_is_split_lane()

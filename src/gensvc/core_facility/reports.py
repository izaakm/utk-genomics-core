import logging
import os
import pandas as pd
import pathlib
import re
import sys
import warnings

# from gensvc.misc import sequencing_run, utils
# from gensvc.data import sequencing_run

from gensvc.misc import utils
from gensvc.data import illumina
# from gensvc.data.illumina import IlluminaSequencingData


logger = logging.getLogger(__name__)


# def find(runid, datadir):
#     if not datadir.is_dir():
#         raise ValueError('not a directory: "{datadir}"')
#     for item in datadir.glob('*'):
#         if item.name == runid:
#             return item


# def find_rundir(runid, miseqdir=None, novaseqdir=None):
#     rundir = find(runid, miseqdir) or find(runid, novaseqdir)
#     return rundir


# def find_procdir(runid, procdir=None):
#     rundir = find(runid, procdir)
#     if rundir and rundir.is_dir():
#         contents = sorted([item for item in rundir.glob('*') if item.is_dir()])
#         return contents[-1]
#     else:
#         return None


def new_procdir(runid, procdir=None):
    if not procdir or not procdir.is_dir():
        raise ValueError(f'not a directory: "{procdir}"')
    return procdir / runid / datetime.now().strftime('%Y%m%dT%H%M%S')


def md5sum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
    


def find_samplesheets(dirname):
    '''
    Search a directory for an Illumina Sample Sheet file.

    [TODO] Sort multiple sample sheets by modified time.
    '''
    # canonical = []
    # real = []
    # symlinks = []

    found = []

    # print(dirname)
    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    # logger.debug(f'Looking for sample sheets in {dirname}')
    logger.debug(f'Looking for sample sheets in {dirname}')
    for path in dirname.glob('*.csv'):
        logger.debug(path)
        if illumina.looks_like_samplesheet(path):
            found.append(path.resolve())
    return sorted(set(found))


def find_seq_runs(dirname):
    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    seq_runs = []
    for path in dirname.iterdir():
        if not path.is_dir():
            continue
        if illumina.is_runid(path.name):
            seq_runs.append(path.resolve())
        else:
            logger.debug(f'No runid found in "{path}"')

    return sorted(seq_runs)


def list_runs(data, long=False, sep=None, transpose=False, as_dataframe=False):
    '''
    List sequencing runs in `dirpath`.

    long : boolean
        Print more stuff about each run.

    [TODO] Add a `--json` option to output JSON.

    [TODO] seems like there is a bug when printing, I think one of the runs has
    no "projects" and this causes the columns to be misaligned. I thought
    adding 'na_rep="-"' would fix this, but it doesn't seem to work.
    '''
    index = False
    header = True
    # data = [seqrun.info for seqrun in seqruns]
    table = pd.DataFrame(data)
    if 'projects' in table:
        table = table.explode(column='projects')

    if transpose:
        table = table.T
        index = True
        header = False

    if as_dataframe:
        return table
    elif sep is None:
        return table.to_string(index=index, header=header, na_rep='-')
    else:
        return table.to_csv(index=index, header=header, sep=sep, na_rep='-')


def cli_list(args):
    '''
    List info for sequencing runs or sample sheets. Possible inputs:

    - A directory containing sequencing runs.
    - A sequencing run directory.
    - A sample sheet.
    '''
    sample_sheets = []
    run_dirs = []
    info = []
    for path in args.pathlist:
        if not os.path.exists(path):
            sys.tracebacklimit = 0
            raise FileNotFoundError(f'Path does not exist: {path}')
        if illumina.looks_like_samplesheet(path):
            logger.debug(f'Found sample sheet: {path}')
            sample_sheets.append(path)
        elif os.path.isdir(path):
            if illumina.is_runid(os.path.basename(path)):
                logger.debug(f'Found runid: {path}')
                run_dirs.append(path)
            else:
                for rundir in find_seq_runs(path):
                    run_dirs.append(rundir)

    for path in sample_sheets:
        try:
            sample_sheet = illumina.read_sample_sheet(path)
            info.append(sample_sheet.info)
        except Exception as e:
            logger.debug(f'Error reading {path}: {e}')
            continue

    for path in run_dirs:
        try:
            seqrun = illumina.IlluminaSequencingData(path)
        except Exception as e:
            logger.debug(f'Error reading {path}: {e}')
            continue
        if not seqrun.path_to_samplesheet.exists():
            sample_sheets = find_samplesheets(path)
            if len(sample_sheets) == 1:
                seqrun.path_to_samplesheet = sample_sheets[0]
            elif len(sample_sheets) > 1:
                logger.debug(f'Multiple sample sheets found in {path}')
                continue
        info.append(seqrun.info)
    # The 'table' is a string.
    table = list_runs(
        info,
        long=args.long,
        transpose=args.transpose,
        sep=args.sep
    )
    print(table)
    return 0


# END

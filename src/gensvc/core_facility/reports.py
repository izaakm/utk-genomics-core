import re
import pathlib
import sys
import warnings
import logging

# from gensvc.misc import sequencing_run, utils
# from gensvc.data import sequencing_run

from gensvc.misc import utils
from gensvc.data import illumina
from gensvc.data.illumina import IlluminaSequencingData


logger = logging.getLogger(__name__)


def find(runid, datadir):
    if not datadir.is_dir():
        raise ValueError('not a directory: "{datadir}"')
    for item in datadir.glob('*'):
        if item.name == runid:
            return item


def find_rundir(runid, miseqdir=None, novaseqdir=None):
    rundir = find(runid, miseqdir) or find(runid, novaseqdir)
    return rundir


def find_procdir(runid, procdir=None):
    rundir = find(runid, procdir)
    if rundir and rundir.is_dir():
        contents = sorted([item for item in rundir.glob('*') if item.is_dir()])
        return contents[-1]
    else:
        return None


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
        try:
            runid = illumina.regex_runid.search(str(path)).group(0)
            seq_runs.append((runid, path))
        except:
            pass

    return sorted(seq_runs, key=lambda item: item[-1])


def list(dirpath, long=False, sep='\t'):
    '''
    List sequencing runs in `dirpath`.

    long : boolean
        Print more stuff about each run.
    '''
    records = []
    short = not long

    for path in dirpath.iterdir():
        # print(path)
        rundir = path.resolve()
        runid = utils.get_runid(rundir)
        if not runid:
            continue

        seqrun = IlluminaSequencingData(rundir)
        # print(seqrun)

        if short:
            records.append(
                {
                    'instrument': seqrun.instrument,
                    'runid': seqrun.runid,
                    'path': str(seqrun.path)
                }
            )
        elif long:
            row = {
                'instrument': seqrun.instrument,
                'runid': seqrun.runid,
                'path': str(seqrun.path)
            }
            pathlist = find_samplesheets(rundir)
            if len(pathlist) > 1:
                warnings.warn(f'Multiple sample sheets found: "{pathlist}"')
                for ss in pathlist:
                    if ss.name == 'SampleSheet.csv':
                        seqrun.path_to_samplesheet = ss
                        break
                else:
                    # Continue to the next sequencing run.
                    warnings.warn(f'Unable to disambiguate sample sheets, skipping {rundir}')
                    continue
            else:
                seqrun.path_to_samplesheet = pathlist[0]

            # print(seqrun)
            # print(seqrun.path_to_samplesheet)
            # print(seqrun.samplesheet.sections)
            # print(seqrun.samplesheet.Header)

            # row['Experiment'] = seqrun.samplesheet.Header['Experiment Name']
            row['experiment'] = seqrun.name
            # row['Project'] = seqrun.samplesheet.sample_project
            row['project'] = seqrun.projects
            # print(row)

            # print(seqrun.samplesheet.FileFormatVersion, seqrun.samplesheet.version)
            # print(illuminadata.samplesheet.path)
            # --- 8<
            # try:
            #     for project in illuminadata.sample_project:
            #         print(f'{illuminadata.instrument:<8} {illuminadata.runid:<35} {illuminadata.info["Experiment Name"]:<40} {project:<40}')
            # except:
            #     print(f'{instrument:<8} {runid:<35} {path}')
            # --- >8
            # print(f'{illuminadata.instrument:<8} {illuminadata.runid:<35} {illuminadata.info["Experiment Name"]:<40} {project:<40}')
            # print(sep.join([illuminadata.instrument, illuminadata.runid, illuminadata.samplesheet.Header.get("Experiment Name"), project]))

            records.append(row)
    return records

# END

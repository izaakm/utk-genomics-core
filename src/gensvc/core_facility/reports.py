import re
import pathlib
import sys

# from gensvc.misc import sequencing_run, utils
from gensvc.data import sequencing_run
from gensvc.misc import utils
from gensvc.data import illumina


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
    


def find_samplesheet(dirname):
    '''
    Search a directory for an Illumina Sample Sheet file.

    [TODO] Sort multiple sample sheets by modified time.
    '''
    canonical = []
    real = []
    symlinks = []

    # print(dirname)
    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    # print(dirname)
    for path in dirname.iterdir():
        # print(path)
        if path.is_file() and path.suffix == '.csv':
            if looks_like_samplesheet(path):
                path = path.absolute()
                if path.name == 'SampleSheet.csv':
                    canonical.append(path)
                elif path.is_symlink():
                    symlinks.append(path)
                else:
                    real.append(path)
    return canonical + real + symlinks


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

def list(dirname, long=False, sep='|'):
    short = not long
    if isinstance(dirname, str):
        dirname = pathlib.Path(dirname)
    for path in dirname.iterdir():
        realpath = path.resolve()
        runid = utils.get_runid(realpath)
        if not runid:
            continue

        if 'MiSeq' in str(realpath):
            instrument = 'MiSeq'
        else:
            instrument = 'NovaSeq'

        # seqrun = sequencing_run.IlluminaSequencingData(
        #     runid=runid,
        #     rundir=path,
        #     instrument=instrument
        # )
        # print(seqrun)

        if short:
            # if realpath == path:
            #     print(instrument, runid, path)
            # else:
            #     print(instrument, runid, path, '->', realpath)
            print(sep.join([instrument, runid, str(path)]))
        elif long:
            try:
                illuminadata = sequencing_run.IlluminaSequencingData(path)
                # illuminadata.find_samplesheet()  # 'find_samplesheet' should be *not* be a method for illumina data; find the samplesheet and then pass it to the IlluminaSequencingData constructor.

                # print(illuminadata.path_to_samplesheet)
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
                for project in illuminadata.projects:
                    # print(sep.join([illuminadata.instrument, illuminadata.runid, illuminadata.samplesheet.Header.get("Experiment Name"), project]))
                    print(
                        sep.join([
                            str(i) for i in (illuminadata.instrument, illuminadata.runid, illuminadata.samplesheet.Header.get("Experiment Name"), project)
                        ])
                    )
            except Exception as e:
                print(f'[ERROR] Cannot processes run "{path}": {e}', file=sys.stderr)
    return None

# END

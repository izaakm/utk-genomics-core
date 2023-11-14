import re
import pathlib

from gensvc.misc import sequencing_run

regex_runid = re.compile('[^\/]*\d{6}[^\/]*')

def get_runid(path):
    try:
        return regex_runid.search(str(path)).group(0)
    except:
        return None

def find_seq_runs(dirname):
    if isinstance(dirname, str):
        dirname = pathlib.Path(dirname)
    seq_runs = []
    for path in dirname.iterdir():
        try:
            runid = regex_runid.search(str(path)).group(0)
            seq_runs.append((runid, path))
        except:
            pass
    return sorted(seq_runs, key=lambda item: item[-1])

def list(dirname):
    if isinstance(dirname, str):
        dirname = pathlib.Path(dirname)
    for path in dirname.iterdir():
        realpath = path.resolve()
        runid = get_runid(realpath)
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

        if realpath == path:
            print(instrument, runid, path)
        else:
            print(instrument, runid, path, '->', realpath)
    return None

# END

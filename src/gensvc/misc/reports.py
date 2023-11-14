import re
import pathlib

from gensvc.misc import sequencing_run

regex_runid = re.compile('[^\/]*\d{6}[^\/]*')

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
    for runid, path in find_seq_runs(dirname):
        # print(path)
        # runid = get_runid(path)
        if not runid:
            continue
        if 'MiSeq' in str(path):
            instrument = 'MiSeq'
        else:
            instrument = 'NovaSeq'
        seqrun = sequencing_run.SeqRun(
            runid=runid,
            rundir=path,
            instrument=instrument
        )
        print(seqrun)

    return None

# END

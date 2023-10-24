import pathlib
import os

_get_path_or_none = lambda name: pathlib.Path(os.getenv(name)) if name in os.environ else None

regex_runid = re.compile('[^\/]*\d{6}[^\/]*')

def get_env():
    if 'GENSVC_DIR' in os.environ:
        GENSVC_DIR = pathlib.Path(os.getenv('GENSVC_DIR'))
        GENSVC_NOVASEQDATA = _get_path_or_none('GENSVC_NOVASEQDATA') or GENSVC_DIR / 'NovaSeqRuns'
        GENSVC_MISEQDATA = _get_path_or_none('GENSVC_MISEQDATA') or GENSVC_DIR / 'MiSeqRuns'
        GENSVC_PROCDATA = _get_path_or_none('GENSVC_PROCDATA') or GENSVC_DIR / 'processed'
    else:
        GENSVC_DIR = None
        GENSVC_NOVASEQDATA = None
        GENSVC_MISEQDATA = None
        GENSVC_PROCDATA = None

def get_runid(path):
    try:
        return regex_runid.search(str(path)).group(0)
    except:
        return None

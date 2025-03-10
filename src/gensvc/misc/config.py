import pathlib
import os


GENSVC_DATADIR = None
GENSVC_MISEQ_DATADIR = None
GENSVC_NEXTSEQ_DATADIR = None
GENSVC_NOVASEQ_DATADIR = None
GENSVC_PROCDATA = None


def get_env():
    _get_path_or_none = lambda name: pathlib.Path(os.getenv(name)) if name in os.environ else None
    env = dict()
    if 'GENSVC_DIR' in os.environ:
        env.update(
            GENSVC_DIR = pathlib.Path(os.getenv('GENSVC_DIR')),
            GENSVC_NOVASEQDATA = _get_path_or_none('GENSVC_NOVASEQDATA') or GENSVC_DIR / 'NovaSeqRuns',
            GENSVC_MISEQDATA = _get_path_or_none('GENSVC_MISEQDATA') or GENSVC_DIR / 'MiSeqRuns',
            GENSVC_PROCDATA = _get_path_or_none('GENSVC_PROCDATA') or GENSVC_DIR / 'processed'
        )
    else:
        env.update(
            GENSVC_DIR = None,
            GENSVC_NOVASEQDATA = None,
            GENSVC_MISEQDATA = None,
            GENSVC_PROCDATA = None
        )

    return env

import pathlib
import os


__package_root__ = pathlib.Path(__file__).parent.parent.resolve()

class Config:
    '''
    GENSVC_DATADIR=/Users/jmill165/data/mirrors/gensvc
    GENSVC_ISEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/iSeqRuns
    GENSVC_NEXTSEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/NextSeqRuns
    GENSVC_NOVASEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/NovaSeqRuns
    GENSVC_PROCDATA=/Users/jmill165/data/mirrors/gensvc/processed
    '''
    _DEBUG = False
    _GENSVC_DATADIR = os.getenv('GENSVC_DATADIR')
    _GENSVC_ILLUMINA_DIR = os.getenv('GENSVC_ILLUMINA_DIR')
    _GENSVC_ISEQ_DATADIR = os.getenv('GENSVC_ISEQ_DATADIR')
    _GENSVC_NEXTSEQ_DATADIR = os.getenv('GENSVC_NEXTSEQ_DATADIR')
    _GENSVC_NOVASEQ_DATADIR = os.getenv('GENSVC_NOVASEQ_DATADIR')
    _GENSVC_PROCDATA = os.getenv('GENSVC_PROCDATA')
    _GENSVC_TRASH_DIR = os.getenv('GENSVC_TRASH_DIR')
    _GENSVC_UTSTOR_DIR = os.getenv('GENSVC_UTSTOR_DIR')
    _SLURM_SUBMIT = False
    _templates = __package_root__.parent / 'templates'

    @property
    def DEBUG(self):
        return self._DEBUG

    @property
    def GENSVC_DATADIR(self):
        return pathlib.Path(self._GENSVC_DATADIR) if self._GENSVC_DATADIR else None

    @property
    def GENSVC_ILLUMINA_DIR(self):
        if self._GENSVC_ILLUMINA_DIR:
            return pathlib.Path(self._GENSVC_ILLUMINA_DIR)
        elif self.GENSVC_DATADIR:
            return self.GENSVC_DATADIR / 'Illumina'
        else:
            return None

    @property
    def GENSVC_ISEQ_DATADIR(self):
        if self._GENSVC_ISEQ_DATADIR:
            return pathlib.Path(self._GENSVC_ISEQ_DATADIR)
        elif self.GENSVC_ILLUMINA_DIR:
            return self.GENSVC_ILLUMINA_DIR / 'iSeqRuns'
        else:
            return None

    @property
    def GENSVC_NEXTSEQ_DATADIR(self):
        if self._GENSVC_NEXTSEQ_DATADIR:
            return pathlib.Path(self._GENSVC_NEXTSEQ_DATADIR)
        elif self.GENSVC_ILLUMINA_DIR:
            return self.GENSVC_ILLUMINA_DIR / 'NEXTSEQRuns'
        else:
            return None

    @property
    def GENSVC_NOVASEQ_DATADIR(self):
        if self._GENSVC_NOVASEQ_DATADIR:
            return pathlib.Path(self._GENSVC_NOVASEQ_DATADIR)
        elif self.GENSVC_ILLUMINA_DIR:
            return self.GENSVC_ILLUMINA_DIR / 'NovaSeqRuns'
        else:
            return None

    @property
    def GENSVC_PROCDATA(self):
        if self._GENSVC_PROCDATA:
            return pathlib.Path(self._GENSVC_PROCDATA)
        elif self.GENSVC_DATADIR:
            return self.GENSVC_DATADIR / 'processed'
        else:
            return None

    @property
    def GENSVC_TRASH_DIR(self):
        if self._GENSVC_TRASH_DIR:
            return pathlib.Path(self._GENSVC_TRASH_DIR)
        elif self.GENSVC_DATADIR:
            return self.GENSVC_DATADIR / '.TRASH'
        else:
            return None

    @property
    def GENSVC_UTSTOR_DIR(self):
        if self._GENSVC_UTSTOR_DIR:
            return pathlib.Path(self._GENSVC_UTSTOR_DIR)
        elif self.GENSVC_DATADIR:
            return self.GENSVC_DATADIR / 'utstor'
        else:
            return None

    @property
    def SLURM_SUBMIT(self):
        return self._SLURM_SUBMIT

    @property
    def templates(self):
        return self._templates

config = Config()


# def get_env():
#     _get_path_or_none = lambda name: pathlib.Path(os.getenv(name)) if name in os.environ else None
#     env = dict()
#     if 'GENSVC_DIR' in os.environ:
#         env.update(
#             GENSVC_DIR = pathlib.Path(os.getenv('GENSVC_DIR')),
#             GENSVC_NOVASEQDATA = _get_path_or_none('GENSVC_NOVASEQDATA') or GENSVC_DIR / 'NovaSeqRuns',
#             GENSVC_MISEQDATA = _get_path_or_none('GENSVC_MISEQDATA') or GENSVC_DIR / 'MiSeqRuns',
#             GENSVC_PROCDATA = _get_path_or_none('GENSVC_PROCDATA') or GENSVC_DIR / 'processed'
#         )
#     else:
#         env.update(
#             GENSVC_DIR = None,
#             GENSVC_NOVASEQDATA = None,
#             GENSVC_MISEQDATA = None,
#             GENSVC_PROCDATA = None
#         )
#     return env

def cli(args):
    '''
    Command line interface for configuration settings.
    '''
    if args.list:
        print('Current configuration settings:')
        for key in dir(config):
            if not key.startswith('_'):
                value = getattr(config, key)
                print(f'{key}={value}')

    return 0

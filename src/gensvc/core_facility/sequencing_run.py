'''
The Illumina instruments from the Genomics Core Facility create data in the
form of a sequencing run ("seqrun"). Each seqrun needs to go through a
conversion step, and the outputs of the conversion are made available to the
core facility's users via Globus.

The `SequencingRun` class takes a "run ID" for a seqrun and has attributes and
methods for accessing all of the data objects associated with that seqrun.
'''


from gensvc.data.base import Datadir
from gensvc.data.illumina import IlluminaSequencingData, read_sample_sheet
from gensvc.misc.config import config


class ProcessedData(Datadir):
    '''
    Represents processed sequencing data.

    >>> proc = ProcessedData('/path/to/processed/251009_VL00838_46_AAHGGYWM5')
    '''
    @property
    def BCLConvert(self):
        '''
        Path to BCLConvert output directory.

        Returns a pathlib.Path object.
        '''
        return self.path / 'BCLConvert'

    @property
    def transfer(self):
        '''
        Path to transfer directory.

        Returns a pathlib.Path object.
        '''
        return self.path / 'transfer'

    @property
    def samplesheet(self):
        '''
        Canonical path to samplesheet file.

        Returns a pathlib.Path object.
        '''
        return self.path / 'SampleSheet.csv'

    def find_samplesheets(self):
        '''
        Find samplesheet files in the processed data directory.

        Returns a list of pathlib.Path objects.

        [TODO] Include 'BCLConvert/Reports/SampleSheet.csv'???
        >>> proc = ProcessedData('/path/to/processed/251009_VL00838_46_AAHGGYWM5')
        >>> proc.find_samplesheets()  # doctest: +ELLIPSIS
        [...]
        '''
        sheets = []
        if ( sheet := self.samplesheet ).exists():
            sheets.append(sheet)
        if ( sheet := self.BCLConvert / 'Reports/SampleSheet.csv' ).exists():
            sheets.append(sheet)
        return sheets


class SequencingRun:
    '''
    Data directories for sequencing run by run ID, including raw and processed data.

    Parameters
    ----------
    run_id : str
        The unique identifier for the sequencing run.
    path_to_rundir : str or pathlib.Path, optional
        The path to the run directory (i.e., raw data). If None, search `illumina_root` for the run ID.
    path_to_procdir : str or pathlib.Path, optional
        The path to the processed data directory. If None, search `processed_root` for the run ID.
    illumina_root : str or pathlib.Path, optional
        The root directory for Illumina sequencing runs (defaults to `config.GENSVC_ILLUMINA_DIR`).
    processed_root : str or pathlib.Path, optional
        The root directory for processed data (defaults to `config.GENSVC_PROCDATA`).

    Methods
    -------
    list_samplesheets()
        List all samplesheet files in both raw and processed data directories.
    get_latest_samplesheet()
        Get the most recently modified samplesheet file.

    Examples
    --------

    >>> run = SequencingRun('251009_VL00838_46_AAHGGYWM5')
    '''
    def __init__(
            self,
            run_id,
            path_to_rundir=None,
            path_to_procdir=None,
            illumina_root=config.GENSVC_ILLUMINA_DIR,
            processed_root=config.GENSVC_PROCDATA,
        ):
        if path_to_rundir is None:
            found = list(illumina_root.glob(f'*Runs/{run_id}'))
            if len(found) == 0:
                # Print warning.
                pass
            elif len(found) == 1:
                path_to_rundir = found[0]
            else:
                raise ValueError(f'Multiple run directories found for {run_id}: {found}')

        if path_to_procdir is None:
            path_to_procdir = processed_root / run_id

        self.run_id = run_id
        self.rundir = IlluminaSequencingData(path_to_rundir)
        self.procdir = ProcessedData(path_to_procdir)
        self.instrument_name = None  # NextSeq, NovaSeq, etc.
        self.instrument_id = None  # Instrument serial number or ID.

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.run_id}")'

    def list_samplesheets(self):
        '''
        >>> run = SequencingRun('251009_VL00838_46_AAHGGYWM5')
        >>> run.list_samplesheets()                             # doctest: +ELLIPSIS
        [...]
        '''
        sheets = self.rundir.find_samplesheets() + self.procdir.find_samplesheets()
        return sheets

    def get_latest_samplesheet(self):
        '''
        >>> run = SequencingRun('251009_VL00838_46_AAHGGYWM5')
        >>> run.get_latest_samplesheet()                        # doctest: +ELLIPSIS
        PosixPath('...')
        '''
        sheets = self.list_samplesheets()
        if len(sheets) == 0:
            return None
        latest_sheet = max(sheets, key=lambda p: p.stat().st_mtime)
        return latest_sheet


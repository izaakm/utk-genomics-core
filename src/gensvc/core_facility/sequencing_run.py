from gensvc.data.base import ProcessedData
from gensvc.data.illumina import IlluminaSequencingData
from gensvc.misc.config import config


class SequencingRun:
    def __init__(
            self,
            run_id,
            path_to_rundir=None,
            path_to_procdir=None,
            illumina_data=config.GENSVC_ILLUMINA_DIR,
            proc_data=config.GENSVC_PROCDATA,
        ):

        if path_to_rundir is None:
            found = list(illumina_data.glob(f'*Runs/{run_id}'))
            if len(found) == 0:
                # Print warning.
                pass
            elif len(found) == 1:
                path_to_rundir = found[0]
            else:
                raise ValueError(f'Multiple run directories found for {run_id}: {found}')

        if path_to_procdir is None:
            path_to_procdir = proc_data / run_id

        self.run_id = run_id
        self.rundir = IlluminaSequencingData(path_to_rundir)
        self.procdir = ProcessedData(path_to_procdir)
        self.instrument_name = None  # NextSeq, NovaSeq, etc.
        self.instrument_id = None  # Instrument serial number or ID.

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.run_id}")'

    def list_samplesheets(self):
        sheets = self.rundir.find_samplesheets() + self.procdir.find_samplesheets()
        return sheets

    def get_latest_samplesheet(self):
        sheets = self.list_samplesheets()
        if len(sheets) == 0:
            return None
        latest_sheet = max(sheets, key=lambda p: p.stat().st_mtime)
        return latest_sheet


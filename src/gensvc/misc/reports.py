import re
import pathlib
import sys

from gensvc.misc import sequencing_run, utils

regex_runid = re.compile(r'[^\/]*\d{6}[^\/]*')

def find_seq_runs(dirname):
    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    seq_runs = []
    for path in dirname.iterdir():
        try:
            runid = regex_runid.search(str(path)).group(0)
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
                illuminadata.find_samplesheet()
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

import shutil

from gensvc.misc import utils
from gensvc.data import base

class TransferData(base.ProcessedData):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

def copy_data(src, dst, dry_run=False):
    if dry_run:
        print(f'[DRYRUN] mkdir -pv {dst}')
        print(f'[DRYRUN] cp -lr {src} -> {dst}')
    else:
        print(f'[NOT IMPLEMENTED] mkdir -pv {dst}')  # Is this necessary w/ shutil?
        print(f'[NOT IMPLEMENTED] cp -lr {src} -> {dst}')
        # https://stackoverflow.com/a/10778930
        # shutil.copytree(src, dst, copy_function=os.link)

def setup_transfer(procdir=None, transfers=None, ignore_names=['Stats', 'Reports', 'all'], dry_run=False, debug=False):
    # TODO: add dry_run
    transfers = transfers or procdir / '_transfers'

    for results_dir in procdir.iterdir():
        # print(results_dir)
        if not results_dir.is_dir():
            print(f'[DEBUG] not a directory: {results_dir}') if debug else None
            continue
        elif not results_dir.name in ['fastq', '_bcl2fastq', 'SummaryStatistics']:
            # Only enabled for certain directories.
            print(f'[DEBUG] name not allowed: {results_dir}') if debug else None
            continue
            
        for results_subdir in results_dir.iterdir():
            if not results_subdir.is_dir():
                print(f'[DEBUG] results_subdir is not a dir: {results_subdir}') if debug else None
                continue
            elif results_subdir.name in ignore_names:
                print(f'[DEBUG] results_subdir name is ignored: {results_subdir}') if debug else None
                continue

            # print(f'[DEBUG] results_subdir: {results_subdir}')
            # Eg: .../processed/231024_M04398_0066_000000000-LBLPL/20231025T120002/fastq/UTK_skuschke_Miller_231024 
            #                                                                            ^ results_subdir = sample_project (current working directory)
            #                                                                      ^ results_dir
            #                                                      ^ procdir
            sample_project = results_subdir.name
            print(f'[DEBUG] sample_project: {sample_project}') if debug else None
            results_dir = results_subdir.parent.name
            print(f'[DEBUG] results_dir: {results_dir}') if debug else None
            dest = transfers / sample_project / results_dir
            copy_data(results_subdir, dest, dry_run=dry_run)

def transfer_data(source, destination, reference_file=None, dry_run=False, debug=False):
    print(f'[NOT IMPLEMENTED] mkdir -pv {destination}')
    print(f'[NOT IMPLEMENTED] cp -r {source} -> {destination}')
    print(f'[NOT IMPLEMENTED] chown --reference={reference_file} {destination} # --reference is only in GNU chown')

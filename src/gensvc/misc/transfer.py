import shutil
from gensvc.misc import utils

def copy_data(src, dst):
    print('source      :', subdir)
    print('destination :', dst)
    # https://stackoverflow.com/a/10778930
    # shutil.copytree(src, dst, copy_function=os.link)

def setup_transfer(seqrun=None, procdir=None, transfers=None, ignore_names=['Stats', 'Reports', 'all']):
    # TODO: add dry_run
    if seqrun:
        # Set default values from seqrun.
        procdir = procdir or seqrun.procdir
        transfers = transfers or seqrun.procdir / 'transfers'

    transfers = transfers or procdir / 'transfers'

    for dir in procdir.iterdir():
        if not dir.name in ['fastq', 'SummaryStatistics']:
            continue
            
        for subdir in dir.iterdir():
            # print(dir)
            if not subdir.is_dir():
                continue
            elif subdir.name in ignore_names:
                continue
            print('source      :', subdir)
        
            project = subdir.name
            group = subdir.parent.name
            dest = transfers / project / group
            print('destination :', dest)
            # https://stackoverflow.com/a/10778930
            # shutil.copytree(src, dst, copy_function=os.link)

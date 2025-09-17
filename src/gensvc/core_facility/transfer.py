import re
import shutil
import sys

from gensvc.misc import utils
from gensvc.misc.config import config
from gensvc.data import base

from jinja2 import Environment, FileSystemLoader

def link_fastq(srcdir, destdir):
    '''
    Hardlink the fastq files from the source directory to the destination directory.

    Path.hardlink_to(target)
        Make this path a hard link to the same file as target.
    '''
    filelist = list(srcdir.glob('*.fastq.gz'))
    links = []
    if len(filelist) == 0:
        return links

    destdir.mkdir(parents=True, exist_ok=True)
    for fastq in filelist:
        target = destdir / fastq.name
        # DRY RUN
        # print('mkdir -p', target.parent)
        # print(target, '->', fastq)
        # Hardlink:
        if target.is_file():
            target.unlink()
        target.hardlink_to(fastq)
        links.append(target)
    return links


def link_csv(srcdir, destdir):
    '''
    Hardlink the csv files from the source directory to the destination directory.
    '''
    for csv in srcdir.glob('*.csv'):
        target = destdir / csv.name
        # DRY RUN
        # print('mkdir -p', target.parent)
        # print(target, '->', csv)
        # Hardlink:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file():
            target.unlink()
        target.hardlink_to(csv)


def make_transfer_commands(runid, sample_project, projectid):
    '''
    Consider adding conditionals for the project account. EG, set read/write
    permissions differently for specific projects.


    Given the following processed data:

        /lustre/isaac24/proj/UTK0192/data/processed/<RUNID>
        /lustre/isaac24/proj/UTK0192/data/processed/<RUNID>/BCLConvert/<SAMPLE_PROJECT>

    Create the transfer script:

        /lustre/isaac24/proj/UTK0192/data/processed/<RUNID>/transfer/<PROJECTID>/transfer-<SAMPLE_PROJECT>.sh

    The transfer script transfers data from the source directory in UTK0192 to the destination project:

        /lustre/isaac24/proj/UTK0192/data/processed/<RUNID>/transfer/<PROJECTID>/<SAMPLE_PROJECT>
        /lustre/isaac24/proj/<PROJECTID>/UTKGenomicsCoreSequencingData/<RUNID>/<SAMPLE_PROJECT>

    Specific directories and files for reference:

        /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5
        /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/BCLConvert/UTK0341_vonarnim_Alex_250507
        /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/transfer/UTK0341/UTK0341_vonarnim_Alex_250507
        /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/transfer/UTK0341/transfer-UTK0341_vonarnim_Alex_250507.sh
        /lustre/isaac24/proj/UTK0341/UTKGenomicsCoreSequencingData/250507_VL00838_25_AAGY5MJM5/UTK0341_vonarnim_Alex_250507
    '''
    if projectid == 'UTK0204':
        # Make all of Beever's files group writable.
        rsync_cmd = 'rsync -auv --no-owner --no-group --chmod=F660 --chmod=D3770 "${srcdir}/" "${dstdir}/"'
    else:
        rsync_cmd = 'rsync -auv --no-owner --no-group --chmod=F640 --chmod=D3750 "${srcdir}/" "${dstdir}/"'
    lines = [
        '#!/usr/bin/env bash',
        '',
        'set -x',
        'set -e',
        'set -u',
        'set -o pipefail',
        '',
        'umask 002',
        '',
        'declare logfile="$(basename -s .sh "$0").log"',
        '',
        '# Redirect stdout and stderr to the logfile.',
        'exec 1>>"$logfile"',
        'exec 2>>"$logfile"',
        '',
        f'declare runid="{runid}"',
        f'declare projectid="{projectid}"',
        f'declare sample_project="{sample_project}"',
        '',
        'declare srcdir="/lustre/isaac24/proj/UTK0192/data/processed/${runid}/transfer/${projectid}/${sample_project}"',
        'declare projdir="/lustre/isaac24/proj/${projectid}"',
        'declare dstdir="/lustre/isaac24/proj/${projectid}/UTKGenomicsCoreSequencingData/${runid}/${sample_project}"',
        '',
        'mkdir -p "${dstdir}"',
        rsync_cmd,
        '',
        '# Fix ownership',
        'chown --reference "${projdir}" "${projdir}/UTKGenomicsCoreSequencingData"',
        'find "${projdir}/UTKGenomicsCoreSequencingData/${runid}" -user $(whoami) -exec chown --reference "${projdir}" {} \\;',
        '',
        '# Verify ownership',
        'find "${projdir}/UTKGenomicsCoreSequencingData/${runid}" -print0 | xargs -0 ls -ld',
        '',
        'touch "${runid}.ISAACTransferComplete"',
        '',
        'exit 0',
    ]
    return '\n'.join(lines)


def cli_setup_transfer_dirs(args):
    '''
    Examples
    --------
    /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5
    /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/BCLConvert/UTK0341_vonarnim_Alex_250507
    /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/transfer/UTK0341/UTK0341_vonarnim_Alex_250507
    /lustre/isaac24/proj/UTK0192/data/processed/250507_VL00838_25_AAGY5MJM5/transfer/UTK0341/transfer-UTK0341_vonarnim_Alex_250507.sh
    /lustre/isaac24/proj/UTK0341/UTKGenomicsCoreSequencingData/250507_VL00838_25_AAGY5MJM5/UTK0341_vonarnim_Alex_250507
    '''
    print('templates:', config.templates)
    environment = Environment(loader=FileSystemLoader(config.templates))
    readme_template = environment.get_template('readme.md')

    procdir = args.procdir.resolve()
    runid = procdir.name

    for dirname in procdir.glob('BCLConvert/*'):
        do_transfer = False
        if not dirname.is_dir():
            continue
        elif dirname.name == 'Logs':
            continue
        elif dirname.name == 'Reports':
            continue

        sample_project = dirname.name

        if re.match(r'^UTK\d{4}', sample_project):
            projectid = re.match(r'^(UTK\d{4})', sample_project).group(1)
            do_transfer = True
        elif re.match(r'^UTK', sample_project):
            projectid = 'unknown_project'
        else:
            projectid = 'external'

        path_to_transfer_fastq = procdir / 'transfer' / projectid / sample_project / 'fastq'
        path_to_transfer_readme = procdir / 'transfer' / projectid / sample_project / 'README.md'
        path_to_transfer_script = procdir / 'transfer' / projectid / f'transfer-{sample_project}.sh'

        if args.dry_run:
            print('[DRY RUN] link files ...')
        else:
            links = link_fastq(dirname, path_to_transfer_fastq)
            # Also link the "Undetermined" fastq files.
            links += link_fastq(dirname.parent, path_to_transfer_fastq)
            print(f'Linked {len(links)} fastq files from "{dirname}" into "{path_to_transfer_fastq}".', file=sys.stderr)
            with open(path_to_transfer_readme, 'w') as f:
                print(readme_template.render(), file=f)

        if do_transfer:
            # Make the transfer script.
            transfer_script = make_transfer_commands(runid, sample_project, projectid)
            if args.dry_run:
                print(f'[DRY RUN] Would write readme to {path_to_transfer_readme}')
                print(f'[DRY RUN] Would write transfer script to {path_to_transfer_script}')
                print(transfer_script)
            else:
                # The directory should already exist *if* there were fastq files.
                path_to_transfer_script.parent.mkdir(parents=True, exist_ok=True)
                with open(path_to_transfer_script, 'w') as f:
                    print(transfer_script, file=f)

    for dirname in procdir.glob('SummaryStatistics/*'):
        # do_transfer = False
        sample_project = dirname.name
        if sample_project == 'all':
            continue
        elif re.match(r'^UTK\d{4}', sample_project):
            project = re.match(r'^(UTK\d{4})', sample_project).group(1)
            # do_transfer = True
        elif re.match(r'^UTK', sample_project):
            project = 'unknown_project'
        else:
            project = 'external'

        transfer_stats = procdir / 'transfer' / project / sample_project / 'SummaryStatistics'

        if args.dry_run:
            print(f"{dirname} -> {transfer_stats}")
        else:
            link_csv(dirname, transfer_stats)

        # Don't worry about writing the transfer script bc there really
        # shouldn't ever be a time where we have STATS but NO DATA.

    return 0

import os
import pathlib
import re
import sys
import logging


from datetime import datetime, timedelta
from gensvc.misc.config import config
from gensvc.misc.templates import templates

logger = logging.getLogger(__name__)

re_rundir = re.compile(r'^(?P<date>\d{6,8})_(?P<inst>[A-Z0-9]+)_(?P<id>\d+)_(?P<flowcell>[A-Z0-9-]+)$')

def get_script_header(trash_dir):
    lines = [
        '#!/bin/bash -l',
        '# Cleanup old sequencing runs',
        '',
        '# set -x',
        'set -e',
        'set -u',
        'set -o pipefail',
        'umask 002',
        '',
        'declare dry_run=false',
        '',
        'run() {',
        '    # Run the command, or just print it if dry_run is true.',
        '    local command=("$@")',
        '    if [[ $dry_run == true ]] ; then',
        '        echo "[DRYRUN] ${command[@]}" ;',
        '    else',
        '        eval "${command[@]}" ;',
        '    fi',
        '}',
        '',
        'while getopts "n" opt ; do',
        '    case "$opt" in',
        '        n) dry_run=true ;;',
        '        *) echo "Unrecognized option: $opt" ; exit 1 ;;',
        '    esac',
        'done',
        '',
        f'run \'mkdir -pv "{trash_dir}"\'',
        '',
        'echo "Trashing sequencing runs older than six months ..."',
        ''

    ]
    return '\n'.join(lines) + '\n'


def cleanup(rundir_ls, archive_dir, trash_dir):
    '''
    Sequencing runs that are older than six months should be removed. Include a
    30 day grace period => 210 days.
    '''
    now = datetime.now()
    retention_ymd = (now - timedelta(days=210)).strftime('%y%m%d')

    script = []
    for rundir in rundir_ls:
        if rundir.is_dir() and re_rundir.match(rundir.name):
            logger.debug('Run directory: %s' % rundir)
        else:
            logger.debug('Skipping: %s' % rundir)
            continue

        run_date = re_rundir.match(rundir.name).group('date')

        if run_date < retention_ymd:
            script.extend([
                f'# Run dir is older than six months: {rundir}',
                f'if [[ -n "$(find {archive_dir} -maxdepth 2 -name "{rundir.name}.archivecomplete" -type d)" ]] ; then',
                f'    run \'mv -iv "{rundir}" "{trash_dir}/"\' ;',
                f'else',
                f'    echo "Skipping {rundir}, not archived yet." ;',
                f'fi\n',
            ])

    return '\n'.join(script) + '\n'


def cli(args):
    if args.verbose >= 2:
        logger.setLevel(logging.DEBUG)
    elif args.verbose == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    logger.debug("Data Directory: %s" % config.GENSVC_DATADIR)
    logger.debug("Illumina Directory: %s" % config.GENSVC_ILLUMINA_DIR)
    logger.debug("UTStoR Directory: %s" % config.GENSVC_UTSTOR_DIR)

    script = get_script_header(config.GENSVC_TRASH_DIR)

    for inst_dir in config.GENSVC_ILLUMINA_DIR.glob('*Runs'):
        if inst_dir.name == 'iSeqRuns':
            # Skip iSeqRuns
            continue
        elif inst_dir.is_dir():
            logger.debug('Instrument Directory:', inst_dir)

            # Returns a single script string.
            new_lines = cleanup(
                inst_dir.iterdir(),
                config.GENSVC_UTSTOR_DIR,
                config.GENSVC_TRASH_DIR
            )
            script += new_lines
        else:
            logger.debug('Skipping:', inst_dir)

    if args.output:
        with open(args.output, 'w') as f:
            print(script, file=f)
    else:
        print(script)


# END

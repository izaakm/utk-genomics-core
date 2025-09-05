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


def archive(rundir_ls, archive_dir):
    '''

    Returns
    -------
    list of tuple
    '''
    now = datetime.now()
    current_ymd = now.strftime('%y%m%d')
    data = []
    for rundir in rundir_ls:
        if rundir.is_dir() and re_rundir.match(rundir.name):
            logger.debug('Run directory: %s' % rundir)
        else:
            logger.debug('Not a run directory, skipping: %s' % rundir)
            continue

        run_date = re_rundir.match(rundir.name).group('date')
        if len(run_date) == 8:
            # iSeq Runs
            yyyy = run_date[0:4]
        else:
            # NovaSeq, NextSeq Runs
            yyyy = '20' + run_date[0:2]

        archive_dst = archive_dir / yyyy / rundir.name

        script_path = archive_dir / yyyy / f'{rundir.name}-archive.sh'

        script = templates['archive.sh'].format(
            __job_name=f'{rundir.name}-archive',
            __rundir=rundir,
            __runid=rundir.name,
            __utstor_dir=archive_dst.parent
        )

        data.append((script_path, script))

    return data


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

    for inst_dir in config.GENSVC_ILLUMINA_DIR.glob('*Runs'):
        if inst_dir.is_dir():
            logger.info('Instrument Directory: %s' % inst_dir)

            # Returns a list of (script_path, script) tuples.
            script_data = archive(inst_dir.iterdir(), config.GENSVC_UTSTOR_DIR)

            for script_path, script_content in script_data:
                with open(script_path, 'w') as f:
                    print(script_content, file=f)
                    logger.info(f'Wrote job script: {script_path}')
                if config.SLURM_SUBMIT:
                    logger.info(f'Submitting job: {script_path}')
                    logger.info(f'TODO sbatch {script_path}')

        else:
            logger.info('Skipping: %s' % inst_dir)

# END

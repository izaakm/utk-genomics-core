import os
import pathlib
import re
import sys

from templates import templates

from datetime import datetime, timedelta

re_rundir = re.compile(r'^(?P<date>\d{6})_(?P<inst>[A-Z0-9]+)_(?P<id>\d+)_(?P<flowcell>[A-Z0-9]+)$')


class Config:
    '''
    GENSVC_DATADIR=/Users/jmill165/data/mirrors/gensvc
    GENSVC_ISEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/iSeqRuns
    GENSVC_NEXTSEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/NextSeqRuns
    GENSVC_NOVASEQ_DATADIR=/Users/jmill165/data/mirrors/gensvc/Illumina/NovaSeqRuns
    GENSVC_PROCDATA=/Users/jmill165/data/mirrors/gensvc/processed
    '''
    datadir = pathlib.Path('~/data/mirrors/gensvc/Illumina').expanduser()
    illumina_dir = pathlib.Path('~/data/mirrors/gensvc/Illumina').expanduser()
    utstor_dir = pathlib.Path('~/data/mirrors/gensvc/utstor').expanduser()
    debug = False
    submit = False


def archive(rundir_ls, archive_dir, debug=False):
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
            if debug:
                print('# Run directory:', rundir, file=sys.stderr)
        else:
            if debug:
                print('# Skipping:', rundir, file=sys.stderr)
            continue

        run_date = re_rundir.match(rundir.name).group('date')
        yyyy = '20' + run_date[0:2]

        archive_dst = archive_dir / yyyy / rundir.name

        # print('# Run directory:', rundir)
        # print('# Archive destination:', archive_dst)

        script_path = archive_dir / yyyy / f'{rundir.name}-archive.sh'
        # print(f'sbatch {script_path}')

        script = templates['archive.sh'].format(
            __job_name=f'{rundir.name}-archive',
            __rundir=rundir,
            __runid=rundir.name,
            __utstor_dir=archive_dst.parent
        )
        # print(script)

        data.append((script_path, script))

    return data


def cleanup(rundir_ls, archive_dir, debug=False):
    '''
    Sequencing runs that are older than six months should be removed.
    '''
    script = []
    now = datetime.now()
    retention_ymd = (now - timedelta(days=180)).strftime('%y%m%d')

    for rundir in rundir_ls:
        if rundir.is_dir() and re_rundir.match(rundir.name):
            if debug:
                print('# Run directory:', rundir, file=sys.stderr)
        else:
            if debug:
                print('# Skipping:', rundir, file=sys.stderr)
            continue

        run_date = re_rundir.match(rundir.name).group('date')

        if run_date < retention_ymd:
            script.extend([
                f'# Run dir is older than six months: {rundir}',
                f'if [[ -n "$(find {archive_dir} -name "{rundir.name}.archivecomplete" -type d)" ]] ; then',
                f'    echo rm -rf "{rundir}" ;',
                f'else',
                f'    echo "# Skipping {rundir}, not archived yet." ;',
                f'fi\n',
            ])

    script = '\n'.join(script)
    return script


if __name__ == '__main__':
    config = Config()
    print("Data Directory:", config.datadir)
    print("Illumina Directory:", config.illumina_dir)
    print("UTStoR Directory:", config.utstor_dir)

    for inst_dir in config.illumina_dir.glob('*Runs'):
        if inst_dir.is_dir():
            print('Instrument Directory:', inst_dir)

            # Returns a list of (script_path, script) tuples.
            script_data = archive(inst_dir.iterdir(), config.utstor_dir, debug=config.debug)
            # print(script)

            for script_path, script_content in script_data:
                with open(script_path, 'w') as f:
                    print(script_content, file=f)
                    print(f'# Wrote job script: {script_path}')
                if config.submit:
                    print(f'# Submitting job: {script_path}')
                    print(f'sbatch {script_path}')

            # Returns a single script string.
            script = cleanup(inst_dir.iterdir(), config.utstor_dir, debug=config.debug)
            print(script)
        else:
            print('Skipping:', inst_dir)

# END

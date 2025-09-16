import os
import subprocess

from simple_slurm import Slurm

from gensvc.misc.config import config

# Example sbatch directives:
# #SBATCH --job-name=bcl-convert-$(basename $bcl_input_directory)
# #SBATCH --account=ISAAC-UTK0192
# #SBATCH --partition=short
# #SBATCH --qos=short
# #SBATCH --ntasks=1
# #SBATCH --cpus-per-task=48
# #SBATCH --time=0-03:00:00
# #SBATCH --output=%x-%j.o
# #SBATCH --mail-type=ALL
# #SBATCH --mail-user=OIT_HPSC_Genomics@utk.edu

slurm = Slurm(
    job_name='gensvc-convert',
    account='ISAAC-UTK0192',
    nodes=1,
    ntasks=1,
    cpus_per_task=48,
    partition='short',
    qos='short',
    time='0-03:00:00',
    output='%x-%j.o',
    mail_type='ALL',
    mail_user='bcs@utk.edu'
)
slurm.set_shell('/bin/bash -l')
slurm.add_cmd('set -e')
slurm.add_cmd('set -u')
slurm.add_cmd('set -o pipefail')
slurm.add_cmd('umask 002')
slurm.add_cmd('ulimit -n 16384')


class BCLConvert:
    def __init__(self,
            path_to_executable=None,
            bcl_input_directory=None,
            output_directory=None,
            sample_sheet=None,
            bcl_sampleproject_subdirectories=True,
            sample_name_column_enabled=True,
            output_legacy_stats=True
        ):
        self._path_to_executable = path_to_executable
        self._bcl_input_directory = bcl_input_directory
        self._output_directory = output_directory
        self._sample_sheet = sample_sheet
        self._bcl_sampleproject_subdirectories = bcl_sampleproject_subdirectories
        self._sample_name_column_enabled = sample_name_column_enabled
        self._output_legacy_stat = output_legacy_stats

    def __repr__(self):
        return self.cmd

    @property
    def path_to_executable(self):
        return self._path_to_executable

    @path_to_executable.setter
    def path_to_executable(self, value):
        self._path_to_executable = value

    @property
    def bcl_input_directory(self):
        return self._bcl_input_directory

    @bcl_input_directory.setter
    def bcl_input_directory(self, value):
        self._bcl_input_directory = value

    @property
    def output_directory(self):
        return self._output_directory

    @output_directory.setter
    def output_directory(self, value):
        self._output_directory = value

    @property
    def sample_sheet(self):
        return self._sample_sheet

    @sample_sheet.setter
    def sample_sheet(self, value):
        self._sample_sheet = value

    @property
    def bcl_sampleproject_subdirectories(self):
        return self._bcl_sampleproject_subdirectories

    @bcl_sampleproject_subdirectories.setter
    def bcl_sampleproject_subdirectories(self, value):
        if not isinstance(value, bool):
            raise ValueError("bcl_sampleproject_subdirectories must be a boolean value")
        self._bcl_sampleproject_subdirectories = value

    @property
    def sample_name_column_enabled(self):
        return self._sample_name_column_enabled

    @sample_name_column_enabled.setter
    def sample_name_column_enabled(self, value):
        if not isinstance(value, bool):
            raise ValueError("sample_name_column_enabled must be a boolean value")
        self._sample_name_column_enabled = value

    @property
    def output_legacy_stats(self):
        return self._output_legacy_stat

    @output_legacy_stats.setter
    def output_legacy_stats(self, value):
        if not isinstance(value, bool):
            raise ValueError("output_legacy_stats must be a boolean value")
        self._output_legacy_stat = value

    @property
    def cmdlist(self):
        cmd = [str(self._path_to_executable)]

        if self.bcl_input_directory:
            cmd += ['--bcl-input-directory', str(self.bcl_input_directory)]

        if self.output_directory:
            cmd += ['--output-directory', str(self.output_directory)]

        if self.sample_sheet:
            cmd += ['--sample-sheet', str(self.sample_sheet)]

        if self.bcl_sampleproject_subdirectories is False:
            cmd += ['--bcl-sampleproject-subdirectories', 'false']
        else:
            cmd += ['--bcl-sampleproject-subdirectories', 'true']

        if self.sample_name_column_enabled is False:
            cmd += ['--sample-name-column-enabled', 'false']
        else:
            cmd += ['--sample-name-column-enabled', 'true']

        if self.output_legacy_stats is False:
            cmd += ['--output-legacy-stats', 'false']
        else:
            cmd += ['--output-legacy-stats', 'true']

        return cmd

    @property
    def cmd(self):
        return ' '.join(self.cmdlist)

    def run(self):
        subprocess.run(self.cmdlist, check=True)

    def srun(self, *args, **kwargs):
        slurm.srun(self.cmdlist, *args, **kwargs)

    def sbatch(self, *args, **kwargs):
        slurm.sbatch(self.cmdlist, *args, **kwargs)


def cli(args):

    if args.bcl_input_directory is None:
        raise ValueError("bcl_input_directory is required")
    run_id = args.bcl_input_directory.name

    if args.output_directory is None:
        # Use defaults.
        proc_dir = config.GENSVC_PROCDATA / run_id
        args.output_directory =  proc_dir / 'BCLConvert'
        job_file = proc_dir / 'bclconvert.sh'
    else:
        proc_dir = args.output_directory.name
        job_file = None

    # print(args)
    
    bclconvert = BCLConvert(
        path_to_executable=args.path_to_bclconvert_exe,
        bcl_input_directory=args.bcl_input_directory,
        output_directory=args.output_directory,
        sample_sheet=args.sample_sheet,
        bcl_sampleproject_subdirectories=args.bcl_sampleproject_subdirectories,
        sample_name_column_enabled=args.sample_name_column_enabled,
        output_legacy_stats=args.output_legacy_stats
    )

    if args.dump:
        print(slurm)
        print(bclconvert)

    if args.run:
        proc_dir.mkdir(parents=True, exist_ok=True)
        bclconvert.run()
    elif args.srun:
        proc_dir.mkdir(parents=True, exist_ok=True)
        slurm.srun(bclconvert.cmd)
    elif args.sbatch:
        proc_dir.mkdir(parents=True, exist_ok=True)
        slurm.sbatch(bclconvert.cmd, job_file=str(job_file))

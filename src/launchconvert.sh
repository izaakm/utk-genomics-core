#!/bin/bash

LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
BATCHNAME=bcl2fastq_v5.slurm
FASTQDIR=./fastq.\$SLURM_JOB_ID
MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu

for rundir in `dirname \`find "$LUSTREDIR" -maxdepth 3 -mtime -1 -name CopyComplete.txt\` 2>/dev/null`; do
    if [ `find "$rundir" -maxdepth 1 -name "$BATCHNAME" | wc -l` -eq 0 ]; then
        if [ `find "$rundir/Alignment_1" -mindepth 2 -maxdepth 2 -type d -name Fastq | wc -l` -eq 0 ]; then
            cat > "$rundir/$BATCHNAME" << SBATCH_EOF
#!/bin/bash
#SBATCH -J bcl
#SBATCH --account ISAAC-UTK0192
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=48
#SBATCH --exclusive=mcs
#SBATCH --partition=short
#SBATCH --time=03:00:00
#SBATCH --error=job.e%J
#SBATCH --output=job.o%J
#SBATCH --qos=short
#SBATCH --mail-type=ALL
#SBATCH --mail-user=$MAILLIST

ulimit -n 16384
FASTQDIR=./fastq.\$SLURM_JOB_ID
module load bcl2fastq2
mkdir \$FASTQDIR
bcl2fastq -p 48 -R ./ -o \$FASTQDIR --sample-sheet ./SampleSheet.csv
SBATCH_EOF
            cd "$rundir"
            if ! [ -f SampleSheet.csv ]; then
                ln -s "`ls *.csv | head -1`" SampleSheet.csv
            fi
            sbatch "$BATCHNAME"
            jobid=`squeue -u $USER | sort -n | tail -1 | awk '{print $1;}'`
            mail -s "sequencing run completed in $rundir" $MAILLIST << SBATCH_MAIL_EOF
Sequencing run has completed and bcl2fastq conversion with jobid $jobid has been queued in $rundir

You will receive additional mail from Slurm when the conversion job starts and when it completes.
SBATCH_MAIL_EOF
        else
            touch "$rundir/$BATCHNAME" # stop cron from repeatedly emailing about same job
            mail -s "sequencing and conversion run completed in $rundir" $MAILLIST << NO_SBATCH_MAIL_EOF
Sequencing run has completed. No bcl2fastq job submitted since Fastq output directory already exists.
NO_SBATCH_MAIL_EOF
        fi
    fi
done

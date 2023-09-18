#!/bin/bash

LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
BATCHNAME=bcl2fastq_v5.slurm
FASTQDIR=./fastq.\$SLURM_JOB_ID
MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu

# for rundir in `dirname \`find "$LUSTREDIR" -maxdepth 3 -mtime -1 -name CopyComplete.txt\` 2>/dev/null`; do
for rundir in `dirname \`find "$LUSTREDIR" -maxdepth 3 -name CopyComplete.txt\` 2>/dev/null`; do
    echo "Checking: $rundir"
    if [ `find "$rundir" -maxdepth 1 -name "$BATCHNAME" | wc -l` -eq 0 ]; then
        # The batch script doesn't exist.
        echo "No batch script found in $rundir"
        if [ `find "$rundir/Alignment_1" -mindepth 2 -maxdepth 2 -type d -name Fastq | wc -l` -eq 0 ]; then
            # It's a NovaSeq run.
            echo "It's a NovaSeq run: $rundir"
            # Make the batch script.
            # Find the sample sheet.
            # sbatch "$BATCHNAME"
            # Send mail.
        else
            # It's a MiSeq run.
            echo "It's a MiSeq run: $rundir"
            # touch "$rundir/$BATCHNAME" # stop cron from repeatedly emailing about same job
            # Send mail.
        fi
    else
        echo "Found batch script, therefore I assume the job is complete."
    fi
done

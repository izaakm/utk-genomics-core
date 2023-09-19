#!/bin/bash

# LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
LUSTREDIR=/Users/johnmiller/data/gensvc/NovaSeqRuns
logfile=/Users/johnmiller/data/gensvc/logs/submit.log
BATCHNAME=bcl2fastq_v5.slurm
FASTQDIR=./fastq.\$SLURM_JOB_ID
MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu


find_sequencing_completed() {
    dirname $(find "$LUSTREDIR" -maxdepth 3 -name CopyComplete.txt) | sort
}


submit() {
    local rundir="$1"
    submission=(sbatch myscript.slurm "$rundir")
    echo "[DRYRUN] ${submission[@]}"
}


is_submitted() {
    local rundir="$1"
    grep "$rundir" "$logfile" | grep -q 'SUCCESS'
}


send_mail() {
    local rundir="$1"
    local jobid="$2"
    local subject="Sequencing complete ($rundir)" 
    local body="Conversion with bcl2fastq has been queued.\n
\n
jobid $jobid \n
rundir $rundir\n
\n
You will receive additional mail from Slurm when the conversion job starts and when it completes."
    _mail=(
        foo
        -s $subject
        "$MAILLIST"
        "$body"
    )
    printf "${_mail[@]}"
}


logger() {
    local msg=("$@")
    echo "$(date +%Y-%m-%dT%H:%M:%S) ${msg[@]}" | tee -a "$logfile"
}


main() {
    mkdir -pv "$(dirname $logfile)"
    for rundir in $(find_sequencing_completed) ; do
        # echo $rundir
        if is_submitted $rundir -eq 0 ; then
            echo "Already submitted, nothing to do: $rundir"
        else
            submit $rundir
            if [[ $? -eq 0 ]] ; then
                logger "SUCCESS Sequencing conversion job submitted to Slurm for run '$rundir'"
            else
                logger "ERROR Sequencing conversion failed for run '$rundir'"
            fi
            # send_mail $rundir JOBID
        fi
    done
}

main

exit 0

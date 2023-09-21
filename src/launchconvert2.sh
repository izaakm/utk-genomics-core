#!/bin/bash

# set -x
set -e

declare help
declare dry_run
declare LUSTREDIR="${LUSTREDIR:-/lustre/isaac/proj/UTK0192/gensvc}"
declare logfile="${LUSTREDIR}/logs/submit.log"
# declare BATCHNAME=bcl2fastq_v5.slurm
# declare FASTQDIR=./fastq.\$SLURM_JOB_ID
# declare MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu
declare MAILLIST=bioinformatics@utk.edu
declare convert_script=/nfs/home/jmill165/projects/oit-hpsc-gensvc/src/convert.slurm
declare miseqdir="${LUSTREDIR}/MiSeqRuns"
declare novaseqdir="${LUSTREDIR}"

# dry_run=true

run() {
	# Run the command, or just print it if dry_run is true.
    local command=("$@")
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] ${command[@]}"
    else
        "${command[@]}"
    fi
}


find_sequencing_completed() {
    local miseqdir
    local novaseqdir
    if [[ -d "${LUSTREDIR}/MiSeqRuns" ]] ; then
        miseqdir="${LUSTREDIR}/MiSeqRuns"
    fi
    if [[ -d "${LUSTREDIR}" ]] ; then
        # TODO This should be at LUSTREDIR/NovaSeqRuns
        novaseqdir="${LUSTREDIR}"
    fi

    found=( $(find "$miseqdir" "$novaseqdir" -maxdepth 2 -name CopyComplete.txt -mtime 3 | sort) )
    if [[ "${#found[@]}" -gt 0 ]] ; then
        dirname "${found[@]}" 
    else
        echo # null
    fi
}


submit() {
    local rundir="$1"
    local outdir="$2"
    submission=(sbatch "$convert_script" -r "$rundir" -o "$outdir")
    # echo "[DRYRUN] ${submission[@]}"
    run "${submission[@]}"
}


already_submitted() {
    local rundir="$1"
    grep "$rundir" "$logfile" | grep -q 'SUCCESS'
}


send_mail() {
    local rundir="$1"
    local jobid="$2"

    # Keep this dryrun for now.
    echo mail -s "sequencing run completed in $rundir" $MAILLIST << SBATCH_MAIL_EOF
Sequencing run has completed and bcl2fastq conversion with jobid $jobid has been queued in $rundir

You will receive additional mail from Slurm when the conversion job starts and when it completes.
SBATCH_MAIL_EOF
}


logger() {
    # local msg=("$@")
    # echo "$(date +%Y-%m-%dT%H:%M:%S) ${msg[@]}" | tee -a "$logfile"

    # TODO log messages should go to stderr AND the logfile.
    # Alternative? Use file descriptor to redirect to file w/o tee.
    local content=("$@")
    msg="$(date +%Y-%m-%dT%H:%M:%S) - ${content[@]}"
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] $msg" >&2
    else
        echo "$msg" >> "$logfile"
        echo "$msg" >&2 # | tee -a "$logfile"
    fi
}


usage() {
	# Print the help message.
    cat << eof 
Usage: $(basename $0) -r <rundir> [hnfost]

    -h 
        Print this help message and exit.

    -n
        Dry run mode. Do not do anything, just print what would be done.
eof
}


main() {

    local outdir
    local rundir
    local run_id

    while getopts ":hnf:o:r:s:t:" opt; do
        case $opt in
            h) help="true"
                ;;
            n) dry_run="true"
                ;;
            *) echo "invalid command: no parameter included with argument $OPTARG"
                ;;
        esac
    done

    if [[ $help == true ]] ; then
        usage
        exit 0
    fi
    
    run mkdir -pv "$(dirname $logfile)"
    for rundir in $(find_sequencing_completed) ; do
        # echo $rundir
        if already_submitted $rundir -eq 0 ; then
            echo "Already submitted, nothing to do: $rundir"
        else
            run_id="$(basename $rundir)"
            outdir="${LUSTREDIR}/processed/${run_id}/$(date +%Y%m%dT%H%M%S)"
            run mkdir -pv "$outdir"
            run cd "$outdir"
            submit "$rundir" "$outdir"
            run cd -
            if [[ $? -eq 0 ]] ; then
                logger "SUCCESS Sequencing conversion job submitted to Slurm for run '$rundir'"
            else
                logger "ERROR Sequencing conversion failed for run '$rundir'"
            fi
            # send_mail $rundir JOBID
        fi
    done

    exit 0
}


main "$@"

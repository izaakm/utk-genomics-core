#!/bin/bash

############################################################
# Check for new sequencing runs and submit conversion jobs.
#
# This script is intended to be run as a cron job to automate the process of
# checking for new sequencing runs.  For any new runs, a new job is submitted
# to convert the BCL data to FASTQ sequencing files using the convert.slurm
# script. Successful submissions are logged to gensvc/logs/convert.log.
############################################################


# set -x
# set -e
set -eo pipefail

declare help
declare dry_run

readonly LUSTREDIR="${LUSTREDIR:-/lustre/isaac/proj/UTK0192/gensvc}"

readonly MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu
#readonly MAILLIST=bioinformatics@utk.edu

readonly logfile="${LUSTREDIR}/logs/submit.log"
readonly miseqdir="${LUSTREDIR}/MiSeqRuns"
readonly novaseqdir="${LUSTREDIR}"
readonly convert_script=/lustre/isaac/proj/UTK0192/gensvc/bin/convert.slurm


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
    local mtime="$1"

    found=( $(find "$miseqdir" "$novaseqdir" -maxdepth 2 -name CopyComplete.txt -mtime -"$mtime" | sort) )
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

    run "${submission[@]}"

    if [[ $? -eq 0 ]] ; then
        logger "SUCCESS Sequencing conversion job submitted to Slurm for run '$rundir'"
    else
        logger "ERROR Sequencing conversion failed for run '$rundir'"
    fi
}


already_submitted() {
    local rundir="$1"
    grep "$rundir" "$logfile" | grep -q 'SUCCESS'
}


send_mail() {
    local run_id="$1"
    local jobid=$(squeue -u $USER | sort -n | tail -1 | awk '{print $1;}')

    # Keep this dryrun for now.
    echo "[DRYRUN]" mail -s "sequencing completed for $run_id" $MAILLIST << SBATCH_MAIL_EOF
Sequencing run has completed and bcl2fastq conversion with jobid $jobid has been queued for $run_id

You will receive additional mail from Slurm when the conversion job starts and when it completes.
SBATCH_MAIL_EOF
}


logger() {
    # TODO log messages should go to stderr AND the logfile.
    # Alternative? Use file descriptor to redirect to file w/o tee.
    local content=("$@")
    msg="$(date +%Y-%m-%dT%H:%M:%S) - ${content[@]}"
    if [[ $dry_run == true ]] ; then
        # Send everything to stdout.
        echo "[DRYRUN] $msg"
    else
        echo "$msg" >> "$logfile"
        echo "$msg" >&2 # | tee -a "$logfile"
    fi
}


usage() {
	# Print the help message.
    cat << eof 
Usage: $(basename $0) [-h] [-n] [-m <mtime>]

Search '$LUSTREDIR' for new sequencinq runs.
If any are found, submit a bcl2fastq conversion job to the scheduler for each
one.

    -h 
        Print this help message and exit.

    -n
        Dry run mode. Do not do anything, just print what would be done.

    -m <mtime>
        Find Illumina runs created in the last <mtime>*24 hours ago. Passed to
        'find ... -mtime <mtime>'. Default is 2.
eof
}


main() {

    local rundir
    local run_id
    local outdir
    local -i mtime=2

    while getopts ":hnm:" opt; do
        case $opt in
            h) help="true"
                ;;
            n) dry_run="true"
                ;;
            m) mtime="$OPTARG"
                ;;
            *) echo "invalid command: no parameter included with argument $OPTARG"
                ;;
        esac
    done

    if [[ $help == true ]] ; then
        usage
        exit 0
    fi

    run cd "$LUSTREDIR"
    # run mkdir -pv "$(dirname $logfile)"
    for rundir in $( find_sequencing_completed $mtime ) ; do
        # echo $rundir
        if [[ ! -d "$rundir" ]] ; then
            echo "[ERROR] Not a directory: $rundir"
        elif already_submitted $rundir -eq 0 ; then
            echo "[INFO] Already submitted, nothing to do: $rundir"
        else
            run_id="$(basename $rundir)"
            outdir="${LUSTREDIR}/processed/${run_id}/$(date +%Y%m%dT%H%M%S)"
            run mkdir -pv "$outdir"
            run cd "$outdir"
            submit "$rundir" "$outdir"
            run cd "$LUSTREDIR"
            send_mail $run_id
        fi
    done

    exit 0
}


main "$@"

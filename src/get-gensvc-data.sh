#!/usr/bin/env bash

set -e

declare dry_run=${dry_run:-true}

declare SCRATCHDIR="/lustre/isaac24/scratch/jmill165"

declare logfile="${SCRATCHDIR}/projects/utk-genomics-core/logs/get-gensvc-data.log"

declare gensvc_data="/lustre/isaac/proj/UTK0192/gensvc"
declare local_gensvc_data="${SCRATCHDIR}/data/raw/gensvc"

declare miseq_data="${gensvc_data}/MiSeqRuns"
declare local_miseq_data="${local_gensvc_data}/MiSeq"

declare novaseq_data="${gensvc_data}/NovaSeqRuns"
declare local_novaseq_data="${local_gensvc_data}/NovaSeq"

declare nextseq_data="${gensvc_data}/NEXTSEQRuns/Runs"
declare local_nextseq_data="${local_gensvc_data}/NextSeq"


run() {
	# Run the command, or just print it if dry_run is true.
    local command=("$@")
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] ${command[@]}"
    else
        eval "${command[@]} | tee -a ${logfile}"
    fi
}


dorsync() {
    local srcdir="${1}"
    local dstdir="${2}"

    run rsync \
        -auv \
        --no-owner \
        --no-perms \
        --exclude='*.fastq.gz' \
        "${srcdir}/" "${dstdir}/"
}


run echo "logging to ${logfile}"

dorsync "${miseq_data}" "${local_miseq_data}" &
dorsync "${novaseq_data}" "${local_novaseq_data}" &
dorsync "${nextseq_data}" "${local_nextseq_data}" &
wait

exit 0

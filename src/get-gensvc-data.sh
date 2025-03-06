#!/usr/bin/env bash

set -e

declare dry_run=${DRY_RUN:-true}

declare SCRATCHDIR="/lustre/isaac24/scratch/jmill165"
declare DATADIR="${DATADIR:-${SCRATCHDIR}/data}"
declare PROJECT_ROOT="${PROJECT_ROOT:-${SCRATCHDIR}/projects/utk-genomics-core}"

declare logfile="${PROJECT_ROOT}/logs/get-gensvc-data.log"

# declare gensvc_data="/lustre/isaac/proj/UTK0192/gensvc"
declare gensvc_data="/lustre/isaac24/proj/UTK0192/data"

declare local_data="${DATADIR}/raw/gensvc"

# declare miseq_data="${gensvc_data}/MiSeqRuns"
# declare local_miseq_data="${local_data}/MiSeq"

# declare iseq_data="${gensvc_data}/iSeqRuns"
declare iseq_data="${gensvc_data}/Illumina/iSeqRuns"
# declare local_iseq_data="${local_data}/iSeq"
declare local_iseq_data="${local_data}/Illumina/iSeqRuns"

# declare novaseq_data="${gensvc_data}/NovaSeqRuns"
declare novaseq_data="${gensvc_data}/Illumina/NovaSeqRuns"
# declare local_novaseq_data="${local_data}/NovaSeq"
declare local_novaseq_data="${local_data}/Illumina/NovaSeqRuns"

# declare nextseq_data="${gensvc_data}/NEXTSEQRuns/Runs"
declare nextseq_data="${gensvc_data}/Illumina/NEXTSEQRuns"
# declare local_nextseq_data="${local_data}/NextSeq"
declare local_nextseq_data="${local_data}/Illumina/NextSeqRuns"

declare processed_data="${gensvc_data}/processed"
declare local_processed_data="${local_data}/processed"

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

    run mkdir -p "${dstdir}"
    run rsync \
        -auv \
        --no-owner \
        --no-perms \
        --exclude='.git' \
        --exclude='*.bin' \
        --exclude='*.bcl' \
        --exclude='*.cbcl' \
        --exclude='*.imf1' \
        --exclude='*.filter' \
        --exclude='*.tif' \
        --exclude='*.fastq.gz' \
        --exclude='*.incomplete' \
        --delete-excluded \
        "jmill165@dtn1.isaac.utk.edu:${srcdir}/" "${dstdir}/"
}


run echo "logging to ${logfile}"

# # Cannot do multiple rsyncs over ssh in parallel bc password.
# dorsync "${processed_data}" "${local_processed_data}" &
# dorsync "${nextseq_data}" "${local_nextseq_data}" &
# dorsync "${novaseq_data}" "${local_novaseq_data}" &
# dorsync "${iseq_data}" "${local_iseq_data}" &
# wait

dorsync "${processed_data}" "${local_processed_data}"

exit 0

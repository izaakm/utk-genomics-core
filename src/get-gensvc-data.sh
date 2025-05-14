#!/usr/bin/env bash

set -x
set -e
set -u
set -o pipefail

declare logfile="logs/$(basename -s .sh "$0")-$(date +%s).log"

# Redirect stdout and stderr to the logfile.
mkdir -p logs
exec 1>>"$logfile"
exec 2>>"$logfile"

declare dry_run=${DRY_RUN:-true}

declare SCRATCHDIR="/lustre/isaac24/scratch/jmill165"

declare PROJECT_ROOT="$(pwd -P)"
declare logfile="${PROJECT_ROOT}/logs/get-gensvc-data.log"

declare utk0192_data="/lustre/isaac24/proj/UTK0192/data"

# declare local_data="${GENSVC_DATADIR:-${SCRATCHDIR}/data/gensvc}"
declare local_data="${HOME}/data/raw/gensvc"

# declare miseq_data="${utk0192_data}/MiSeqRuns"
# declare local_miseq_data="${local_data}/MiSeq"

declare iseq_data="${utk0192_data}/Illumina/iSeqRuns"
declare local_iseq_data="${local_data}/Illumina/iSeqRuns"

declare novaseq_data="${utk0192_data}/Illumina/NovaSeqRuns"
declare local_novaseq_data="${local_data}/Illumina/NovaSeqRuns"

declare nextseq_data="${utk0192_data}/Illumina/NEXTSEQRuns"
declare local_nextseq_data="${local_data}/Illumina/NextSeqRuns"

declare processed_data="${utk0192_data}/processed"
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
        --exclude='Firmware' \
        --exclude='Thumbnail_Images' \
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

# dorsync "${iseq_data}" "${local_iseq_data}"
# dorsync "${nextseq_data}" "${local_nextseq_data}"
# dorsync "${novaseq_data}" "${local_novaseq_data}"
# dorsync "${processed_data}" "${local_processed_data}"

include_files() {
    echo "Illumina/"
    echo "processed/"
}

# run rsync \
#     --dry-run \
#     -auv \
#     --no-owner \
#     --no-perms \
#     --files-from=<(include_files) \
#     --recursive \
#     --exclude='.Trash*' \
#     --exclude='.git' \
#     --exclude='*.bin' \
#     --exclude='*.bcl' \
#     --exclude='*.cbcl' \
#     --exclude='*.imf1' \
#     --exclude='*.filter' \
#     --exclude='*.tif' \
#     --exclude='*.fastq.gz' \
#     --exclude='*.incomplete' \
#     --exclude='Firmware' \
#     --exclude='Thumbnail_Images' \
#     --delete-excluded \
#     "jmill165@dtn1.isaac.utk.edu:${utk0192_data}/" "${local_data}/"

# run rsync \
#     --dry-run \
#     -auv \
#     --no-owner \
#     --no-perms \
#     --include="*/" \
#     --include="*.csv" \
#     --include="*.txt" \
#     --exclude="*" \
#     --delete-excluded \
#     "jmill165@dtn1.isaac.utk.edu:${utk0192_data}/" "${local_data}/"

run rsync \
    --dry-run \
    -au \
    -vv \
    --no-owner \
    --no-perms \
    --exclude='.Trash*' \
    --exclude='.git' \
    --exclude='*.bin' \
    --exclude='*.bcl' \
    --exclude='*.cbcl' \
    --exclude='*.imf1' \
    --exclude='*.filter' \
    --exclude='*.tif' \
    --exclude='*.fastq.gz' \
    --exclude='*.incomplete' \
    --exclude='Firmware' \
    --exclude='Thumbnail_Images' \
    --delete-excluded \
    "jmill165@dtn1.isaac.utk.edu:${utk0192_data}/" "${local_data}/"





exit 0

#!/usr/bin/env bash

set -eo pipefail

GENSVC_NOVASEQ_DATADIR="/lustre/isaac/proj/UTK0192/gensvc/NovaSeqRuns"
GENSVC_MISEQ_DATADIR="/lustre/isaac/proj/UTK0192/gensvc/MiSeqRuns"
GENSVC_PROCESSED_DATADIR="/lustre/isaac/proj/UTK0192/gensvc/processed"
GENSVC_ARCHIVEDIR="/lustre/isaac/proj/UTK0192/gensvc/archive"

# echo $GENSVC_ARCHIVEDIR

archived=(
    $GENSVC_ARCHIVEDIR/*.tar
)

# echo "${archived[@]}"

echo "Total archived runs :" ${#archived[@]}
echo "Disk usage          :" $(du -csh ${archived[@]} | tail -n 1 )

# Sanity check
# echo "${archived[@]}"
# du -sch "${archived[@]}"

exit 0

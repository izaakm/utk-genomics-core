#!/usr/bin/env bash

set -e

transfer() {
    local sourcedir="$1"
    local destdir="$2"
    local projectdir="$3"
    local datadir="$4"

    echo "#######################################################"
    echo "# BEGIN TRANSER                                       #"
    echo "# sourcedir  : $sourcedir"
    echo "# destdir    : $destdir"
    echo "# projectdir : $projectdir"
    echo "# datadir    : $datadir"

    echo mkdir -pv "$destdir"
    echo rsync -avuP "${sourcedir}/" "${destdir}/"
    echo chown -R --reference "$projectdir" "$datadir"

    echo "# END OF TRANSFER                                     #"
    echo "#######################################################"
}

# sourcedir=/lustre/isaac/proj/UTK0192/gensvc/processed/231214_A01770_0062_BHWTNYDSX7/20231218T165107/transfer/SAMPLE_PROJECT
# destdir=/lustre/isaac/proj/UTK9999/NovaSeqRuns/231214_A01770_0062_BHWTNYDSX7/SAMPLE_PROJECT
# projectdir=/lustre/isaac/proj/UTK9999
# datadir=/lustre/isaac/proj/UTK9999/NovaSeqRuns
# 
# transfer \
#     "$sourcedir" \
#     "$destdir" \
#     "$projectdir" \
#     "$datadir"

exit 0

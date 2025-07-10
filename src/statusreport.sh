#!/usr/bin/env bash

# set -x
set -e
set -u
set -o pipefail

declare datadir="/Users/jmill165/data/raw/gensvc"
declare illumina_dir="${datadir}/Illumina"
declare proc_data="${datadir}/processed"
declare -a run_dirs=( ${datadir}/Illumina/*Runs/*{A01770,FS10003266,VL00838}* )
declare -i width=36

# echo "${run_dirs[@]}"

copy_status() {
    local runid="$1"
    local run_dir=$(find ${illumina_dir}/*Runs -maxdepth 1 -type d -name ${runid} -print)
    local completed="${run_dir}/CopyComplete.txt"
    # echo ${completed}
    # test -f $completed
    if [[ -f $completed ]]; then
        # echo "${runid} | Raw data: completed"
        printf "%-${width}s | %s\n" "${runid}" "Raw data: completed"
    else
        # echo "${runid} | Raw data: INCOMPLETE"
        printf "%-${width}s | %s\n" "${runid}" "Raw data: INCOMPLETE"
    fi
}

bclconvert_status() {
    local runid="$1"
    local bclconvert_dir="${datadir}/processed/${runid}/BCLConvert"
    local completed="${bclconvert_dir}/Logs/FastqComplete.txt"
    local inprogress="${bclconvert_dir}.inprogress"
    local submitted="${bclconvert_dir}/BCLConvertSubmitComplete"
    if [[ -f $inprogress ]] ; then
        # Check 'in progress' first. It's possible that a 'completed' file exists from a previous run.
        # echo "${runid} | BCLConvert: IN PROGRESS"
        printf "%-${width}s | %s\n" "${runid}" "BCLConvert: IN PROGRESS"
    elif [[ -f $completed ]] ; then
        # Then check for completion.
        # echo "${runid} | BCLConvert: completed"
        printf "%-${width}s | %s\n" "${runid}" "BCLConvert: completed"
    elif [[ -f $submitted ]] ; then
        # echo "${runid} | BCLConvert: SUBMITTED"
        printf "%-${width}s | %s\n" "${runid}" "BCLConvert: SUBMITTED"
    else
        # echo "${runid} | BCLConvert: NOT STARTED"
        printf "%-${width}s | %s\n" "${runid}" "BCLConvert: NOT STARTED"
    fi
}

transfer_status() {
    # Transfer directories are setup as:
    # <processed>/<runid>/transfer/<destination>/<sample_project>
    local runid="$1"
    local -a sample_project_dirs=( $(find ${proc_data}/${runid}/transfer/*/* -maxdepth 0 -type d -print 2>/dev/null) )
    local sample_project_dir
    local sample_project_name
    local transfer_complete
    if [[ ${#sample_project_dirs[@]} -eq 0 ]]; then
        # echo "${runid} | Transfer: NOT STARTED"
        printf "%-${width}s | %s\n" "${runid}" "Transfer: NOT STARTED"
        return
    fi
    for sample_project_dir in "${sample_project_dirs[@]}"; do
        sample_project_name=$(basename "${sample_project_dir}")
        notification_complete="${sample_project_dir}.EmailNotification.txt"
        # echo "Sample Project Dir: ${sample_project_dir}"
        if [[ $sample_project_name == *globus* ]] ; then
            transfer_complete="${sample_project_dir}.GlobusTransferComplete"
            collection_complete="${sample_project_dir}.GlobusCollectionComplete"
            printf "%-${width}s | %s\n" "${runid}" "Globus Transfer ${sample_project_name}"
        elif [[ $sample_project_name == *external* ]] ; then
            transfer_complete="${sample_project_dir}.GlobusTransferComplete"
            collection_complete="${sample_project_dir}.GlobusCollectionComplete"
            if [[ -f "$notification_complete" ]] ; then
                printf "%-${width}s | %s: All Complete\n" "${runid}" "External Transfer ${sample_project_name}"
            elif [[ -f "$collection_complete" ]] ; then
                # echo "${runid} | External Transfer ${sample_project_name}: Collection Complete"
                printf "%-${width}s | %s: Collection Complete\n" "${runid}" "External Transfer ${sample_project_name}"
            elif [[ -f "$transfer_complete" ]] ; then
                # echo "${runid} | External Transfer ${sample_project_name}: Transfer Complete"
                printf "%-${width}s | %s: Transfer Complete\n" "${runid}" "External Transfer ${sample_project_name}"
            else
                # echo "${runid} | External Transfer ${sample_project_name}: INCOMPLETE"
                printf "%-${width}s | %s: INCOMPLETE\n" "${runid}" "External Transfer ${sample_project_name}"
            fi
        elif [[ $sample_project_name == UTK* ]] ; then
            transfer_complete="${sample_project_dir}.ISAACTransferComplete"
            if [[ -f "$notification_complete" ]] ; then
                printf "%-${width}s | %s: All Complete\n" "${runid}" "ISAAC Transfer ${sample_project_name}"
            elif [[ -f "$transfer_complete" ]] ; then
                # echo "${runid} | ISAAC Transfer ${sample_project_name}: Transfer Complete"
                printf "%-${width}s | %s: Transfer Complete\n" "${runid}" "ISAAC Transfer ${sample_project_name}"
            else
                # echo "${runid} | ISAAC Transfer ${sample_project_name}: INCOMPLETE"
                printf "%-${width}s | %s: INCOMPLETE\n" "${runid}" "ISAAC Transfer ${sample_project_name}"
            fi
        else
            # echo "${runid} | Transfer ${sample_project_dir}"
            printf "%-${width}s | %s\n" "${runid}" "Transfer ${sample_project_name}"
        fi
    done
}

for run_dir in "${run_dirs[@]}"; do
    runid=$(basename "${run_dir}")
    copy_status "${runid}"
    bclconvert_status "${runid}"
    transfer_status "${runid}"
done

#!/usr/bin/env bash

# set -x
set -e
set -u
set -o pipefail

declare -r datadir="/Users/jmill165/data/raw/gensvc"
declare -r illumina_data="${datadir}/Illumina"
declare -r proc_data="${datadir}/processed"
declare -r globus_data="${datadir}/globus"
declare -a run_dirs=( ${datadir}/Illumina/*Runs/*{A01770,FS10003266,VL00838}* )
declare -i width=36

# echo "${run_dirs[@]}"

copy_status() {
    local runid="$1"
    local run_dir=$(find ${illumina_data}/*Runs -maxdepth 1 -type d -name ${runid} -print)
    local completed="${run_dir}/CopyComplete.txt"
    local fmtstr="%-${width}s | 1) %s\n"
    # echo ${completed}
    if [[ -f $completed ]]; then
        # echo "${runid} | Raw data: completed"
        printf "$fmtstr" "${runid}" "Raw data | completed"
    else
        # echo "${runid} | Raw data: INCOMPLETE"
        printf "$fmtstr" "${runid}" "Raw data | INCOMPLETE"
    fi
}

bclconvert_status() {
    local runid="$1"
    local bclconvert_dir="${datadir}/processed/${runid}/BCLConvert"
    local completed="${bclconvert_dir}/Logs/FastqComplete.txt"
    local inprogress="${bclconvert_dir}.inprogress"
    local submitted="${bclconvert_dir}/BCLConvertSubmitComplete"
    local fmtstr="%-${width}s | 2) %s\n"
    if [[ -f $inprogress ]] ; then
        # Check 'in progress' first. It's possible that a 'completed' file exists from a previous run.
        printf "$fmtstr" "${runid}" "BCLConvert | IN PROGRESS"
    elif [[ -f $completed ]] ; then
        # Then check for completion.
        printf "$fmtstr" "${runid}" "BCLConvert | completed"
    elif [[ -f $submitted ]] ; then
        printf "$fmtstr" "${runid}" "BCLConvert | SUBMITTED"
    else
        printf "$fmtstr" "${runid}" "BCLConvert | NOT STARTED"
    fi
}

transfer_status() {
    # Transfer directories are setup as:
    # <processed>/<runid>/transfer/<destination>/<sample_project>
    local runid="$1"
    local -a sample_project_dirs=( $(find ${proc_data}/${runid}/transfer/*/* -maxdepth 0 -type d -print 2>/dev/null) )
    local fmtstr="%-${width}s | 3) %s\n"
    local sample_project_dir
    local sample_project_name
    local transfer_complete
    if [[ ${#sample_project_dirs[@]} -eq 0 ]]; then
        printf "$fmtstr" "${runid}" "Transfer | NOT STARTED"
        return
    fi
    for sample_project_dir in "${sample_project_dirs[@]}"; do
        ncomp=0
        sample_project_name=$(basename "${sample_project_dir}")
        notification_complete="${sample_project_dir}.EmailNotification.txt"
        # echo "Sample Project Dir: ${sample_project_dir}"
        if [[ $sample_project_name == *globus* ]] ; then
            transfer_complete="${sample_project_dir}.GlobusTransferComplete"
            collection_complete="${globus_data}/${runid}/${sample_project_name}.collection.json"
            if [[ -f "$notification_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Notification Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Notification INCOMPLETE"
            fi
            if [[ -f "$collection_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Collection Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Collection INCOMPLETE"
            fi
            if [[ -f "$transfer_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Transfer Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Transfer INCOMPLETE"
            fi
            printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | ${ncomp} of 3 complete"
        elif [[ $sample_project_dir == *external* ]] ; then
            transfer_complete="${sample_project_dir}.GlobusTransferComplete"
            collection_complete="${globus_data}/${runid}/${sample_project_name}.collection.json"
            if [[ -f "$notification_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Notification Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Notification INCOMPLETE"
            fi
            if [[ -f "$collection_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Collection Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Collection INCOMPLETE"
            fi
            if [[ -f "$transfer_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Transfer Complete"
            else
                printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | Transfer INCOMPLETE"
            fi
            printf "$fmtstr" "${runid}" "External Transfer ${sample_project_name} | ${ncomp} of 3 complete"
        elif [[ $sample_project_name == UTK* ]] ; then
            transfer_complete="${sample_project_dir}.ISAACTransferComplete"
            if [[ -f "$notification_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "ISAAC Transfer ${sample_project_name} | Notification Complete"
            else
                printf "$fmtstr" "${runid}" "ISAAC Transfer ${sample_project_name} | Notification INCOMPLETE"
            fi
            if [[ -f "$transfer_complete" ]] ; then
                ((ncomp+=1))
                printf "$fmtstr" "${runid}" "ISAAC Transfer ${sample_project_name} | Transfer Complete"
            else
                printf "$fmtstr" "${runid}" "ISAAC Transfer ${sample_project_name} | Transfer INCOMPLETE"
            fi
            printf "$fmtstr" "${runid}" "ISAAC Transfer ${sample_project_name} | ${ncomp} of 2 complete"
        else
            printf "$fmtstr" "${runid}" "Transfer ${sample_project_name} | UNKNOWN TRANSFER TYPE"
        fi
    done
}

for run_dir in "${run_dirs[@]}"; do
    if [[ -f "${run_dir}/NOOP" ]] ; then
        echo "Skipping ${run_dir} due to NOOP file." 1>&2
        continue
    fi
    runid=$(basename "${run_dir}")
    copy_status "${runid}"
    bclconvert_status "${runid}"
    transfer_status "${runid}"
done

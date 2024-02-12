#!/usr/bin/env bash

set -e

declare dry_run=${dry_run:-true}

echo "dry_run=$dry_run"

run() {
	# Run the command, or just print it if dry_run is true.
    local command=("$@")
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] ${command[@]}"
    else
        "${command[@]}"
    fi
}


setup_fastq_dir() {
    for dir in fastq/* ; do
        if [[ ! -d $dir ]] ; then
            continue
        elif [[ $dir =~ Reports ]] ; then
            continue
        elif [[ $dir =~ Stats ]] ; then
            continue
        fi
        echo "Directory name is $dir"
        sample_project=$(basename $dir)
        dest="transfer/$sample_project/fastq"
        run mkdir -pv "$dest"
        # Copy the *contents* of the directory, not the directory.
        run cp -lr --update --verbose "$dir/"* "$dest/"
        run cp -l --update --verbose fastq/Undetermined*.fastq.gz "$dest"
    done
}


setup_stats_dir() {
    for dir in SummaryStatistics/* ; do
        if [[ ! -d $dir ]] ; then
            continue
        elif [[ $(basename $dir) == all ]] ; then
            continue
        fi
        echo "Directory name is $dir"
        sample_project=$(basename $dir)
        dest="transfer/$sample_project/SummaryStatistics"
        run mkdir -pv "$dest"
        # Copy the *contents* of the directory, not the directory.
        run cp -lr --update --verbose "$dir/"* "$dest/"
    done
}


setup_transfer_script() {
    local source_dir
    local dest_dir
    local project_dir
    local data_dir
    local realpath
    local runid
    local sample_project
    local transfer_script

    for dir in transfer/* ; do
        if [[ ! -d $dir ]] ; then
            continue
        elif [[ $dir =~ Reports ]] ; then
            continue
        elif [[ $dir =~ Stats ]] ; then
            continue
        fi
        # echo "# [DEBUG] Directory name is $dir"

        # Resolve symlinks.
        realpath="$(pwd -P)"
        # Remove the head of the path.
        runid="${realpath##/lustre/isaac/proj/UTK0192/gensvc/processed/}"
        # Remove the tail of the path.
        runid="${runid%%/*}"

        # Careful, don't get these variables out of order!!!
        sample_project=$(basename $dir)
        transfer_script="transfer-${sample_project}.sh"
        project_account=${sample_project%%_*}
        project_dir="/lustre/isaac/proj/${project_account}"
        data_dir="${project_dir}/UTKGenomicsCoreSequencingData"
        dest_dir="${data_dir}/${runid}/${sample_project}"
        source_dir="${realpath}/transfer/${sample_project}"

        # [TODO] Write script to run with sudo.
        # if [[ -f "$transfer_script" ]] ; then
        #     mv "$transfer_script" "${transfer_script}-$(date +%s).bak"
        # fi
        # touch "$transfer_script"

        echo "#######################################################" 
        echo "# [DEBUG--${runid}/${sample_project}] BEGIN TRANSER"
        echo "# [DEBUG--${runid}/${sample_project}] runid           : $runid"
        echo "# [DEBUG--${runid}/${sample_project}] sample_project  : $sample_project"
        echo "# [DEBUG--${runid}/${sample_project}] project_account : $project_account"
        echo "# [DEBUG--${runid}/${sample_project}] source_dir      : $source_dir"
        if [[ "$project_account" =~ UTK ]] ; then
            echo "# [DEBUG--${runid}/${sample_project}] dest_dir        : $dest_dir"
            echo "# [DEBUG--${runid}/${sample_project}] data_dir        : $data_dir"
            echo "# [DEBUG--${runid}/${sample_project}] project_dir     : $project_dir"
            if [[ -d "$project_dir" ]] ; then
                echo mkdir -pv "$dest_dir"
                echo rsync -avuP "${source_dir}/" "${dest_dir}/"
                echo chown -R --reference "$project_dir" "$data_dir"
                echo "# END OF TRANSFER"
            else
                echo "echo \"[ERROR--${runid}/${sample_project}] Project directory does not exist: $project_dir\""
                echo "echo \"[ERROR--${runid}/${sample_project}] Directory name is $dir\""
            fi
        else
            echo "echo \"[WARNING--${runid}/${sample_project}] External transfer; nothing to do\""
            echo "echo \"[WARNING--${runid}/${sample_project}] Directory name is $dir\""
            echo "echo \"[WARNING--${runid}/${sample_project}] Transfer source is $source_dir\""
        fi
        echo "#######################################################"
    done

}

main() {
    setup_fastq_dir
    setup_stats_dir
    setup_transfer_script > do-transfer.sh
}

main

exit 0

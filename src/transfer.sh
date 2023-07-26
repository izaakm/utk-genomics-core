#!/usr/bin/env bash

# TRANSFER_SOURCE_DIR="$2"
# TRANSFER_DESTINATION_DIR="$3"

function usage() {
    echo "transfer.sh -u USER -g GROUP -s SOURCE -d DESTINATION"
}

if [[ -z "$@" ]] ; then
    usage
    exit 1
fi

# Get the options
while getopts ":s:d:u:g:" opt; do
    case $opt in
        s) source_directory=$OPTARG ;;
        d) destination_directory=$OPTARG ;;
        u) user=$OPTARG ;;
        g) group=$OPTARG ;;
        \?) echo "Invalid option: -$OPTARG" >&2
            usage
            exit 1
    esac
done

errors=0
# Check that all the required options are set
if [ ! -d "$source_directory" ]; then
    echo "Source directory must be an existing directory" >&2
    ((errors+=1))
else
    source_directory="$(cd $source_directory && pwd -P)"
fi

if [ -z "$destination_directory" ]; then
    echo "Destination directory must be specified" >&2
    ((errors+=1))
fi

if [ -z "$user" ]; then
    echo "The target user must be specified" >&2
    ((errors+=1))
fi

if [ -z "$destination_directory" ]; then
    echo "The target group must be specified" >&2
    ((errors+=1))
fi

if [[ $errors -ne 0 ]] ; then
    usage
    echo "Exiting due to errors" >&2
    exit 1
fi

# Make destination
# mkdir -pv "$destination_directory"
echo "mkdir -pv $destination_directory"

# Copy the files
# rsync -avh --chown=$user:$group "${source_directory}/" "${destination_directory}/"
echo "COMMAND: rsync -avh --chown=$user:$group ${source_directory}/ ${destination_directory}/"

# Check the sizes
# du -sh "$source_directory" "$destination_directory"
echo "du -sh $source_directory $destination_directory"

# Success
# echo "Files copied successfully"

exit 0

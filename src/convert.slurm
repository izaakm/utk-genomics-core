#!/bin/bash
#SBATCH --job-name=gensvc-convert
#SBATCH --account=ISAAC-UTK0192
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=48
#SBATCH --exclusive=mcs
#SBATCH --partition=short
#SBATCH --qos=short
#SBATCH --time=0-03:00:00
#SBATCH --output=job-%J-%x.o
#SBATCH --mail-type=ALL
#SBATCH --mail-user=bioinformatics@utk.edu

# set -x
set -e

declare help
declare dry_run
declare run_id
# dry_run=true

# ulimit -n 16384

# LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
# fastqdir=./fastq.\$SLURM_JOB_ID
# sequencing_datadir=/Users/johnmiller/data/gensvc
# logfile=/Users/johnmiller/data/gensvc/logs/convert.log

declare sequencing_datadir="${SEQUENCING_DATADIR:-/lustre/isaac/proj/UTK0192/gensvc}"
declare logfile="${sequencing_datadir}/logs/convert.log"

declare MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu

logger() {
    # TODO log messages should go to stderr AND the logfile.
    # Alternative? Use file descriptor to redirect to file w/o tee.
    local content=("$@")
    msg="$(date +%Y-%m-%dT%H:%M:%S) - $run_id - ${content[@]}"
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] $msg" >&2
    else
        echo "$msg" >> "$logfile"
        echo "$msg" >&2 # | tee -a "$logfile"
    fi
}


usage() {
	# Print the help message.
    cat << eof 
Usage: $(basename $0) -r <rundir> [hnfost]

    -h 
        Print this help message and exit.

    -n
        Dry run mode. Do not do anything, just print what would be done.

    -f <fastqdir>   
        bcl2fastq will output fastq files into this directory (will be created).
        The default for this script is to set this to <outdir>/fastq. Usually,
		-f and -o should be treated as mutually exclusive options, but this is
		not enforced.

    -o <outdir>     
        The main output directory.  The default is to use

            ${sequencing_datadir}/processed/<run_id>/<YYYYmmdd>T<HHMMSS>

        where <run_id> is the basename of the <rundir> (below). This directory
        will be created. Usually, -f and -o should be treated as mutually
		exclusive options, but this is not enforced.

    -r <rundir>     
        Path to Illumina run (must already exist).

    -s <samplesheet>
        Path to SampleSheet.csv (must already exist).
        The default is to look for <rundir>/SampleSheet.csv. If this does not
        exist, and there is exactly one csv file in <rundir>, then that file
        will be used. Otherwise, an error will be returned.

    -t <threads>    
        Number of processing threads for bcl2fastq. Defaults to all available threads.
eof
}


run() {
	# Run the command, or just print it if dry_run is true.
    local command=("$@")
    if [[ $dry_run == true ]] ; then
        echo "[DRYRUN] ${command[@]}"
    else
        "${command[@]}"
    fi
}


make_dir() {
	# Make the directory and its parents.
    local command=(
        mkdir -pv "$1"
    )

    run "${command[@]}"
}


run_bcl2fastq() {
	# Run bcl2fastq.
    local rundir="$1"
    local samplesheet="$2"
    local fastqdir="$3"
    local threads="$4"
    local command=(
        bcl2fastq
        --processing-threads "$threads"
        --runfolder-dir "$rundir"
        --sample-sheet "$samplesheet"
        --output-dir "$fastqdir"
    )

    run source /usr/share/Modules/init/bash
    run module load bcl2fastq2
    #                        ^ It has two 2s!!!

    run "${command[@]}"
}


get_rundir() {
    # Check that rundir exists, and if so, get the full path.
    local rundir="$1"

    if [[ -d "$rundir" ]] ; then
        rundir="$(cd $rundir && pwd -P)"
        echo "$rundir"
    else
        logger "[ERROR] rundir must be an existing directory; you gave: $rundir"
        return 1
    fi
}


get_samplesheet() {
    # If the passed samplesheet exists, use it. Otherwise, check the rundir for a csv file. If there's more than 1 csv file, return an error.
    local rundir="$1"
    local samplesheet="$2"

    if [[ -f "$samplesheet" ]] ; then
        echo "$samplesheet"
    elif [[ -f "${rundir}/SampleSheet.csv" ]] ; then
        echo "${rundir}/SampleSheet.csv"
    else
        csv_files=($(find "$rundir" -maxdepth 1 -name '*.csv'))
        if [[ "${#csv_files[@]}" -eq 0 ]] ; then
            logger "[ERROR] No sample sheet found in rundir"
            exit 1
        elif [[ "${#csv_files[@]}" -eq 1 ]] ; then
            echo "${csv_files[0]}"
        else
            logger "[ERROR] Too many csv files in rundir, samplesheet is ambiguous"
            exit 1
        fi
    fi
}


get_fastqdir() {
	# Use fastqdir if provided, otherwise default to <outdir>/fastq.
    local outdir="$1"
    local fastqdir="$2"

    if [[ -n "$fastqdir" ]] ; then
        echo "$fastqdir" 
    else
        echo "${outdir}/fastq"
    fi
}


main() {
    # Check for arguments.
    if [[ "$#" -eq 0 ]] ; then
        usage
        exit 2
    fi

    local fastqdir
    local outdir
    # local run_id
    local rundir
    local samplesheet
    local threads

    while getopts ":hnf:o:r:s:t:" opt; do
        case $opt in
            h) help="true"
                ;;
            n) dry_run="true"
                ;;
            f) fastqdir="$OPTARG"
                ;;
            o) outdir="$OPTARG"
                ;;
            r) rundir="$OPTARG"
                ;;
            s) samplesheet="$OPTARG"
                ;;
            t) threads="$OPTARG"
                ;;
            *) echo "invalid command: no parameter included with argument $OPTARG"
                ;;
        esac
    done

	# Debugging:
    # echo $help
    # echo $dry_run
    # echo $fastqdir
    # echo $outdir
    # echo $run_id
    # echo $rundir
    # echo $samplesheet
    # echo $threads

    if [[ $help == true ]] ; then
        usage
        exit 0
    fi

    rundir="$(get_rundir $rundir)"
    threads="${threads:-$(nproc)}"

	# Depends on rundir
    run_id="$(basename $rundir)"
    samplesheet="${samplesheet:-$(get_samplesheet $rundir $samplesheet )}"

	# Depends on run_id
    outdir="${outdir:-${sequencing_datadir}/processed/${run_id}/$(date +%Y%m%dT%H%M%S)}"

	# Depends on outdir
    fastqdir="$(get_fastqdir $outdir $fastqdir )"

	# fastqdir may or may not be a subdirectory of outdir.
    make_dir "$outdir"
    make_dir "$fastqdir"

    run_bcl2fastq "$rundir" "$samplesheet" "$fastqdir" "$threads"

    logger "bcl2fastq conversion complete: $fastqdir"

    exit 0
}


main "$@"

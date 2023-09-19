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

set -x

# ulimit -n 16384

# LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
# rootdir=/lustre/isaac/proj/UTK0192/gensvc
# fastqdir=./fastq.\$SLURM_JOB_ID
rootdir=/Users/johnmiller/data/gensvc
logfile=/Users/johnmiller/data/gensvc/logs/convert.log
MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu


errors=0

logger() {
    # TODO log messages should go to stdout AND the logfile.
    local msg=("$@")
    echo "$(date +%Y-%m-%dT%H:%M:%S) ${msg[@]}" | tee -a "$logfile"
}


usage() {
    echo
    echo "Usage: bcl2fastq.slurm <rundir> <samplesheet> <fastqdir>"
    echo "  <rundir>       path to Illumina run (must already exist)"
    echo "  <samplesheet>  path to SampleSheet.csv (must already exist)"
    echo "  <fastqdir>     bcl2fastq will output fastq files into this directory (will be created)"
    echo
}


make_dir() {
    local command=(
        mkdir -pv "$1"
    )
    echo "[DRYRUN] ${command[@]}"
}

run_bcl2fastq() {
    # source /usr/share/Modules/init/bash
    # module load bcl2fastq2  # It has two 2s!!!

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
    echo "[DRYRUN] ${command[@]}"
}


get_rundir() {
    local rundir="$1"
    # Check that rundir exists.
    if [[ -d "$rundir" ]] ; then
        echo "$rundir"
    else
        logger "[ERROR] rundir must be an existing directory; you gave: $rundir"
        ((errors++))
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
            # exit 1
            ((errors++))
        elif [[ "${#csv_files[@]}" -eq 1 ]] ; then
            echo "${csv_files[0]}"
        else
            logger "[ERROR] Too many csv files in rundir, samplesheet is ambiguous"
            # exit 1
            ((errors++))
        fi
    fi
}


get_fastqdir() {
    local outmain="$1"
    local fastqdir="$2"

    # Make the fastq directory.
    if [[ -d "$fastqdir" ]] ; then
        echo "$fastqdir" 
    else
        echo "${outmain}/fastq"
    fi
}


main() {
    # Check for arguments.
    if [[ "$#" -eq 0 ]] ; then
        usage
        exit 2
    fi

    local rundir
    local samplesheet
    local fastqdir
    local threads
    local run_id
    local outmain

    rundir="${1:-$(get_rundir $1)}"
    samplesheet="${2:-$(get_samplesheet $rundir $2 )}"

    run_id="$(basename $rundir)"
    outmain="${rootdir}/processed/${run_id}/$(date +%Y%m%dT%H%M%S)"

    fastqdir="${3:-$(get_fastqdir $outmain $3 )}"
    threads="${4:-$(nproc)}"

    # Exit if there are errors above.
    if [[ errors -ne 0 ]] ; then
        logger "[INFO] Exiting due to errors (see above)"
        usage
        exit 1
    fi

    # mkdir -pv $fastqdir
    make_dir "$fastqdir"
    run_bcl2fastq "$rundir" "$samplesheet" "$fastqdir" "$threads"
}

main "$@"

exit 0

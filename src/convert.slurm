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

############################################################
# Convert Illumina BCL data to FASTQ and generate summary statistics.
#
# The intended use is that this script is called from the launchconvert.sh
# script, which is run as a cron job to automatically identify new sequencing
# runs for conversion.  The principle function of this script is to run
# bcl2fastq to convert BCL data into FASTQ sequence files.  It provides
# additional functions for generating summary statistics that can be shared
# with users.  It also provides a dry-run feature for testing if it is run
# manually.
############################################################


# set -x
# set -e
set -eo pipefail

# Increase the maximum number of files that one process can open because
# bcl2fastq opens lots of files.
ulimit -n 16384

# LUSTREDIR=/lustre/isaac/proj/UTK0192/gensvc
# fastqdir=./fastq.\$SLURM_JOB_ID

declare help
declare dry_run
declare run_id

readonly MAILLIST=OIT_HPSC_Genomics@utk.edu,genomicscore@utk.edu,rkuster@utk.edu

readonly sequencing_datadir="${SEQUENCING_DATADIR:-/lustre/isaac/proj/UTK0192/gensvc}"
readonly logfile="${sequencing_datadir}/logs/convert.log"

readonly ANACONDA3_SH="${ANACONDA3_SH:-/sw/isaac/applications/anaconda3/2021.05/rhel8_gcc10.2.0/anaconda3-2021.05/etc/profile.d/conda.sh}"
readonly condaenv=/lustre/isaac/proj/UTK0192/gensvc/conda/envs/gensvc
readonly extract_illumina_stats=/lustre/isaac/proj/UTK0192/gensvc/bin/extract-bcl2fastq-stats.py


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


copy_samplesheet() {
    local sourcefile="$1"
    local outdir="$2"
    local destination="${outdir}/$(basename $samplesheet)"
    run cp -v "$sourcefile" "$destination"
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

    run mkdir -pv "$fastqdir"
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
        exit 1
    fi
}


get_samplesheet() {
    # If the passed samplesheet exists, use it. Otherwise, check the rundir for
    # a csv file. If there's more than 1 csv file, return an error.
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
    # Use fastqdir for output if provided, otherwise default to <outdir>/fastq.
    local outdir="$1"
    local fastqdir="$2"

    if [[ -n "$fastqdir" ]] ; then
        echo "$fastqdir" 
    else
        echo "${outdir}/fastq"
    fi
}


run_extract_illumina_stats() {

    local fastqdir="$1"
    local outdir="$2"

    if [[ ! -d "$condaenv" ]] ; then
        logger "[WARNING] Conda env does not exist: $conda"
    fi

    if [[ ! -f "$extract_illumina_stats" ]] ; then
        logger "[WARNING] Illumina stats script does not exist: $extract_illumina_stats"
    fi

    if [[ ! -f "$ANACONDA3_SH" ]] ; then
        logger "[WARNING] Anaconda initialization script does not exist: $ANACONDA3_SH"
    fi

    # # run module unload PE-intel
    # run module purge
    # run module load anaconda3
    # run source $ANACONDA3_SH
    # run conda activate
    # run conda activate $condaenv

    # usage: extract-bcl2fastq-stats.py [-h] [--statsfile STATSFILE]
    #                           [--outdir OUTDIR] [--dry-run] fastqdir
    # 
    # Parse stats from bcl2fastq and split by project. Tables for split lane runs
    # will be saved in separate directories according to the "Project" name.
    # 
    # positional arguments:
    #   fastqdir              The bcl2fastq output directory containing the
    #                         "Reports" and "Stats" subdirectories.
    # 
    # options:
    #   -h, --help            show this help message and exit
    #   --statsfile STATSFILE
    #                         Optional: Statistics json file generated by bcl2fastq,
    #                         eg "<bcl2fastq output>/Stats/Stats.json". If
    #                         --fastqdir is provided, this defaults to
    #                         "<fastqdir>/Stats/Stats.json".
    #   --outdir OUTDIR       Directory in which to put the output files.
    #   --dry-run             Just print the output.

    run $extract_illumina_stats $fastqdir --outdir $outdir/summary-stats
}



# run_fastqc() { # TODO }


# run_multiqc() { # TODO }


main() {
    # Check for arguments.
    if [[ "$#" -eq 0 ]] ; then
        usage
        exit 2
    fi

    local fastqdir
    local outdir
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
    run mkdir -pv "$outdir"

    copy_samplesheet "$samplesheet" "$outdir"

    run_bcl2fastq "$rundir" "$samplesheet" "$fastqdir" "$threads"

    logger "bcl2fastq conversion complete: $fastqdir"

    # TODO Fix python environment.
    # run_extract_illumina_stats "$fastqdir" "$outdir"

    exit 0
}


main "$@"

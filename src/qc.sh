#!/usr/bin/env bash

# set -x
set -e

echo $SHELL

if [[ -z "$1" ]] ; then
    echo "Usage: qc.sh RUN_DIR"
    exit 2
fi

module unload PE-intel
module load anaconda3
source $ANACONDA3_SH
conda activate
conda activate gensvc

# module load bcl2fastq2
module load fastqc

# multiqc="$HOME/.conda/envs/gensvc/bin/multiqc"

function logger() {
    echo "[$(date +%FT%T)]" "${@}"
}

DATADIR="/lustre/isaac/proj/UTK0192/gensvc"
PROCESSED_DIR="${DATADIR}/processed"

# Inputs
RUN_DIR="$1"
SAMPLE_SHEET="${RUN_DIR}/SampleSheet.csv"

# Outputs
OUT_DIR="${PROCESSED_DIR}/$(basename $RUN_DIR)/$(date +%s)"
BCL2FASTQ_DIR="${OUT_DIR}/bcl2fastq"
FASTQC_DIR="${OUT_DIR}/qc/fastqc"
MULTIQC_DIR="${OUT_DIR}/qc/multiqc"

if [[ -h $SAMPLE_SHEET ]] ; then
    SAMPLE_SHEET=$(readlink $SAMPLE_SHEET)
fi

if [[ -h $RUN_DIR ]] ; then
    RUN_DIR=$(readlink $RUN_DIR)
fi

if [[ ! -d "$PROCESSED_DIR" ]] ; then
    logger "ERROR: Cannot find PROCESSED_DIR: $PROCESSED_DIR"
    exit 1
fi

if [[ ! -f "$SAMPLE_SHEET" ]] ; then
    logger "ERROR: Cannot find SAMPLE_SHEET: $SAMPLE_SHEET"
    exit 1
fi

logger "RUN_DIR       : $RUN_DIR"
logger "SAMPLE_SHEET  : $SAMPLE_SHEET"
logger "OUT_DIR       : $OUT_DIR"
logger "BCL2FASTQ_DIR : $BCL2FASTQ_DIR"
logger "FASTQC_DIR    : $FASTQC_DIR"
logger "MULTIQC_DIR   : $MULTIQC_DIR"

# # Run bcl2fastq to convert BCL data to fastq files.
# mkdir -pv "$BCL2FASTQ_DIR"
# 
# bcl2fastq \
#     --processing-threads 1 \
#     --runfolder-dir "$RUN_DIR" \
#     --sample-sheet  "$SAMPLE_SHEET" \
#     --output-dir "$BCL2FASTQ_DIR"


# Run FastQC on the fastq.gz files in the bcl2fastq output directory.
mkdir -pv "$FASTQC_DIR"

# find "$BCL2FASTQ_DIR" -name '*.fastq.gz' | xargs fastqc --outdir "$FASTQC_DIR"
find "$RUN_DIR" -name '*.fastq.gz' | xargs fastqc --outdir "$FASTQC_DIR"


# Run MultiQC to gather all of the results from bcl2fastq and fastqc.
mkdir -pv "$MULTIQC_DIR"

multiqc --outdir "$MULTIQC_DIR" --interactive "$RUN_DIR" "$OUT_DIR"

exit 0

########################################################################
# BCL to FASTQ file converter
# bcl2fastq v2.20.0.422
# Copyright (c) 2007-2017 Illumina, Inc.
#
# Usage:
#       bcl2fastq [options]
#
# Command-line options:
#   -h [ --help ]                                   produce help message and exit
#   -v [ --version ]                                print program version information
#   -l [ --min-log-level ] arg (=INFO)              minimum log level recognized values: NONE, FATAL, ERROR, WARNING, INFO, DEBUG, TRACE
#   -i [ --input-dir ] arg (=<runfolder-dir>/Data/Intensities/BaseCalls/) path to input directory
#   -R [ --runfolder-dir ] arg (=./)                path to runfolder directory
#   --intensities-dir arg (=<input-dir>/../)        path to intensities directory If intensities directory is specified, --input-dir must also be specified.
#   -o [ --output-dir ] arg (=<input-dir>)          path to demultiplexed output
#   --interop-dir arg (=<runfolder-dir>/InterOp/)   path to demultiplexing statistics directory
#   --stats-dir arg (=<output-dir>/Stats/)          path to human-readable demultiplexing statistics directory
#   --reports-dir arg (=<output-dir>/Reports/)      path to reporting directory
#   --sample-sheet arg (=<runfolder-dir>/SampleSheet.csv) path to the sample sheet
#   -r [ --loading-threads ] arg (=4)               number of threads used for loading BCL data
#   -p [ --processing-threads ] arg                 number of threads used for processing demultiplexed data
#   -w [ --writing-threads ] arg (=4)               number of threads used for
#                                                   writing FASTQ data.  This should not be set higher than the number of
#                                                   samples.  If set =0 then these threads will be placed in the same pool as
#                                                   the loading threads, and the number of shared threads will be determined by
#   --loading-threads.
#   --tiles arg                                     comma-separated list of regular expressions to select only a subset of the tiles available in the flow-cell.  Multiple entries allowed, each applies to the corresponding base-calls.
#                                                   For example:
#                                                    * to select all the tiles ending with '5' in all lanes:
#                                                        --tiles [0-9][0-9][0-9]5
#                                                    * to select tile 2 in lane 1 and all the tiles in the other lanes:
#                                                        --tiles s_1_0002,s_[2-8]
#   --minimum-trimmed-read-length arg (=35)         minimum read length after adapter trimming
#   --use-bases-mask arg                            specifies how to use each cycle.
#   --mask-short-adapter-reads arg (=22)            smallest number of remaining bases (after masking bases below the minimum trimmed read length) below which whole read is masked
#   --adapter-stringency arg (=0.9)                 adapter stringency
#   --ignore-missing-bcls                           assume 'N'/'#' for missing calls
#   --ignore-missing-filter                         assume 'true' for missing filters
#   --ignore-missing-positions                      assume [0,i] for missing positions, where i is incremented starting from 0
#   --ignore-missing-controls                       (deprecated) assume 0 for missing controls
#   --write-fastq-reverse-complement                generate FASTQs containing reverse complements of actual data
#   --with-failed-reads                             include non-PF clusters
#   --create-fastq-for-index-reads                  create FASTQ files also for index reads
#   --find-adapters-with-sliding-window             find adapters with simple sliding window algorithm
#   --no-bgzf-compression                           turn off BGZF compression for FASTQ files
#   --fastq-compression-level arg (=4)              zlib compression level (1-9) used for FASTQ files
#   --barcode-mismatches arg (=1)                   number of allowed mismatches per index Multiple, comma delimited, entries allowed. Each entry is applied to the corresponding index; last entry applies to all remaining indices.  Accepted values: 0, 1, 2.
#   --no-lane-splitting                             do not split fastq files by lane.
########################################################################
#
#             FastQC - A high throughput sequence QC analysis tool
#
# SYNOPSIS
#
# 	fastqc seqfile1 seqfile2 .. seqfileN
#
#     fastqc [-o output dir] [--(no)extract] [-f fastq|bam|sam]
#            [-c contaminant file] seqfile1 .. seqfileN
#
# DESCRIPTION
#
#     FastQC reads a set of sequence files and produces from each one a quality
#     control report consisting of a number of different modules, each one of
#     which will help to identify a different potential type of problem in your
#     data.
#
#     If no files to process are specified on the command line then the program
#     will start as an interactive graphical application.  If files are provided
#     on the command line then the program will run with no user interaction
#     required.  In this mode it is suitable for inclusion into a standardised
#     analysis pipeline.
#
#     The options for the program as as follows:
#
#     -h --help       Print this help file and exit
#
#     -v --version    Print the version of the program and exit
#
#     -o --outdir     Create all output files in the specified output directory.
#                     Please note that this directory must exist as the program
#                     will not create it.  If this option is not set then the
#                     output file for each sequence file is created in the same
#                     directory as the sequence file which was processed.
#
#     --casava        Files come from raw casava output. Files in the same sample
#                     group (differing only by the group number) will be analysed
#                     as a set rather than individually. Sequences with the filter
#                     flag set in the header will be excluded from the analysis.
#                     Files must have the same names given to them by casava
#                     (including being gzipped and ending with .gz) otherwise they
#                     won't be grouped together correctly.
#
#     --nano          Files come from nanopore sequences and are in fast5 format. In
#                     this mode you can pass in directories to process and the program
#                     will take in all fast5 files within those directories and produce
#                     a single output file from the sequences found in all files.
#
#     --nofilter      If running with --casava then don't remove read flagged by
#                     casava as poor quality when performing the QC analysis.
#
#     --extract       If set then the zipped output file will be uncompressed in
#                     the same directory after it has been created.  By default
#                     this option will be set if fastqc is run in non-interactive
#                     mode.
#
#     -j --java       Provides the full path to the java binary you want to use to
#                     launch fastqc. If not supplied then java is assumed to be in
#                     your path.
#
#     --noextract     Do not uncompress the output file after creating it.  You
#                     should set this option if you do not wish to uncompress
#                     the output when running in non-interactive mode.
#
#     --nogroup       Disable grouping of bases for reads >50bp. All reports will
#                     show data for every base in the read.  WARNING: Using this
#                     option will cause fastqc to crash and burn if you use it on
#                     really long reads, and your plots may end up a ridiculous size.
#                     You have been warned!
#
#     --min_length    Sets an artificial lower limit on the length of the sequence
#                     to be shown in the report.  As long as you set this to a value
#                     greater or equal to your longest read length then this will be
#                     the sequence length used to create your read groups.  This can
#                     be useful for making directly comaparable statistics from
#                     datasets with somewhat variable read lengths.
#
#     -f --format     Bypasses the normal sequence file format detection and
#                     forces the program to use the specified format.  Valid
#                     formats are bam,sam,bam_mapped,sam_mapped and fastq
#
#     -t --threads    Specifies the number of files which can be processed
#                     simultaneously.  Each thread will be allocated 250MB of
#                     memory so you shouldn't run more threads than your
#                     available memory will cope with, and not more than
#                     6 threads on a 32 bit machine
#
#     -c              Specifies a non-default file which contains the list of
#     --contaminants  contaminants to screen overrepresented sequences against.
#                     The file must contain sets of named contaminants in the
#                     form name[tab]sequence.  Lines prefixed with a hash will
#                     be ignored.
#
#     -a              Specifies a non-default file which contains the list of
#     --adapters      adapter sequences which will be explicity searched against
#                     the library. The file must contain sets of named adapters
#                     in the form name[tab]sequence.  Lines prefixed with a hash
#                     will be ignored.
#
#     -l              Specifies a non-default file which contains a set of criteria
#     --limits        which will be used to determine the warn/error limits for the
#                     various modules.  This file can also be used to selectively
#                     remove some modules from the output all together.  The format
#                     needs to mirror the default limits.txt file found in the
#                     Configuration folder.
#
#    -k --kmers       Specifies the length of Kmer to look for in the Kmer content
#                     module. Specified Kmer length must be between 2 and 10. Default
#                     length is 7 if not specified.
#
#    -q --quiet       Supress all progress messages on stdout and only report errors.
#
#    -d --dir         Selects a directory to be used for temporary files written when
#                     generating report images. Defaults to system temp directory if
#                     not specified.
#
# BUGS
#
#     Any bugs in fastqc should be reported either to simon.andrews@babraham.ac.uk
#     or in www.bioinformatics.babraham.ac.uk/bugzilla/
#
#
########################################################################
#
#  MultiQC | v1.14
#
#  Usage: multiqc [OPTIONS] [ANALYSIS DIRECTORY]
#
#  MultiQC aggregates results from bioinformatics analyses across many samples into a single report.
#  It searches a given directory for analysis logs and compiles a HTML report. It's a general use tool, perfect for summarising the output from numerous
#  bioinformatics tools.
#  To run, supply with one or more directory to scan for analysis results. For example, to run in the current working directory, use 'multiqc .'
#
#  Main options
# --force            -f  Overwrite any existing reports
# --config           -c  Specific config file to load, after those in MultiQC dir / home dir / working dir. (PATH)
# --cl-config            Specify MultiQC config YAML on the command line (TEXT)
# --filename         -n  Report filename. Use 'stdout' to print to standard out. (TEXT)
# --outdir           -o  Create report in the specified output directory. (TEXT)
# --ignore           -x  Ignore analysis files (GLOB EXPRESSION)
# --ignore-samples       Ignore sample names (GLOB EXPRESSION)
# --ignore-symlinks      Ignore symlinked directories and files
# --file-list        -l  Supply a file containing a list of file paths to be searched, one per row
#
#  Choosing modules to run
# --module     -m  Use only this module. Can specify multiple times. (MODULE NAME)
# --exclude    -e  Do not use this module. Can specify multiple times. (MODULE NAME)
# --tag            Use only modules which tagged with this keyword (TEXT)
# --view-tags      View the available tags and which modules they load
#
#  Sample handling
# --dirs           -d   Prepend directory to sample names
# --dirs-depth     -dd  Prepend n directories to sample names. Negative number to take from start of path. (INTEGER)
# --fullnames      -s   Do not clean the sample names (leave as full file name)
# --fn_as_s_name        Use the log filename as the sample name
# --replace-names       TSV file to rename sample names during report generation (PATH)
#
#  Report customisation
# --title            -i  Report title. Printed as page header, used for filename if not otherwise specified. (TEXT)
# --comment          -b  Custom comment, will be printed at the top of the report. (TEXT)
# --template         -t  Report template to use. (default|default_dev|gathered|geo|sections|simple)
# --sample-names         TSV file containing alternative sample names for renaming buttons in the report (PATH)
# --sample-filters       TSV file containing show/hide patterns for the report (PATH)
# --custom-css-file      Custom CSS file to add to the final report (PATH)
#
#  Output files
# --flat          -fp  Use only flat plots (static images)
# --interactive   -ip  Use only interactive plots (in-browser Javascript)
# --export        -p   Export plots as static images in addition to the report
# --data-dir           Force the parsed data directory to be created.
# --no-data-dir        Prevent the parsed data directory from being created.
# --data-format   -k   Output parsed data in a different format. (tsv|json|yaml)
# --zip-data-dir  -z   Compress the data directory.
# --no-report          Do not generate a report, only export data and plots
# --pdf                Creates PDF report with the 'simple' template. Requires Pandoc to be installed.
#
#  MultiQC behaviour
# --verbose           -v  Increase output verbosity. (INTEGER RANGE)
# --quiet             -q  Only show log warnings
# --lint                  Use strict linting (validation) to help code development
# --profile-runtime       Add analysis of how long MultiQC takes to run to the report
# --no-megaqc-upload      Don't upload generated report to MegaQC, even if MegaQC options are found
# --no-ansi               Disable coloured log output
# --version               Show the version and exit.
# --help              -h  Show this message and exit.
#
#
#  See http://multiqc.info for more details.
#
########################################################################

#!/bin/bash -l
#SBATCH --job-name gensvc-bclconvert-{{ run_id }}
#SBATCH --account ISAAC-UTK0192
#SBATCH --partition short
#SBATCH --qos short
#SBATCH --ntasks 1
#SBATCH --cpus-per-task 48
#SBATCH --time 0-03:00:00
#SBATCH --output %x-%j.o
#SBATCH --mail-type ALL
#SBATCH --mail-user OIT_HPSC_Genomics@utk.edu

set -x
set -e
set -u
set -o pipefail

umask 002

ulimit -n 16384

declare -r sample_sheet="{{ sample_sheet }}"
declare -r bclconvert_dir="{{ bclconvert_dir }}"
declare -r bclconvert_inprogress="${bclconvert_dir}.inprogress"
declare -r bclconvert_exe="{{ bclconvert_exe }}" 

mkdir -p "${outdir}"
cd "${outdir}"

if [[ -d "$bclconvert_dir" ]] ; then
    echo "BCLConvert dir already exists: $bclconvert_dir"
    exit 1
elif [[ -d "$bclconvert_inprogress" ]] ; then
    echo "BCLConvert is currently IN PROGRESS: $bclconvert_inprogress"
    exit 1
fi

"${bclconvert_exe}" \
    --bcl-input-directory "${bcl_input_directory}" \
    --output-directory "${bclconvert_inprogress}" \
    --sample-sheet "${sample_sheet}" \
    --bcl-sampleproject-subdirectories true \
    --sample-name-column-enabled true \
    --output-legacy-stats true

mv "$bclconvert_inprogress" "$bclconvert_dir"

# ${extract_stats_exe} . --debug --outdir SummaryStatistics
gensvc extract-bcl2fastq-stats BCLConvert/Reports/legacy --outdir SummaryStatistics

find "${outdir}" -user $(whoami) | xargs -n1 -P0 chmod g+w

exit 0

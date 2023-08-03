#!/bin/bash
#SBATCH --job-name gensvc-convert
#SBATCH --account ISAAC-UTK0192
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=56
#SBATCH --exclusive=mcs
#SBATCH --partition=condo-ut-genomics
#SBATCH --time=0-08:00:00
#SBATCH --output=job.o%J
#SBATCH --qos=genomics
#SBATCH --mail-type=ALL
#SBATCH --mail-user=bioinformatics@utk.edu

# ulimit -n 16384

# Options.
runid="230731_A01770_0044_AHHF22DRX3"
version="$(date +%s)"

# Inputs.
rundir="/lustre/isaac/proj/UTK0192/data/processed/${runid}/${version}/runfolder_dir"
samplesheet="/lustre/isaac/proj/UTK0192/data/processed/${runid}/${version}/UTK_ORNL_splitlane230731B.csv" # [TODO] Find this or have the user specify?

# Outputs.
fastqdir="/lustre/isaac/proj/UTK0192/data/processed/${runid}/${version}/fastq"
fastqc_results="/lustre/isaac/proj/UTK0192/data/processed/${runid}/${version}/fastqc"

echo "rundir      : $rundir"
echo "samplesheet : $samplesheet"
echo "fastqdir    : $fastqdir"

sleep 1

source /usr/share/Modules/init/bash
module load bcl2fastq2
mkdir -v $fastqdir

bcl2fastq \
    --processing-threads 48 \
    --runfolder-dir "$rundir" \
    --output-dir "$fastqdir" \
    --sample-sheet "$samplesheet"

module unload bcl2fastq2
module load fastqc

find "$fastqdir" -name '*.fastq.gz' \
    | xargs fastqc \
    --outdir "$fastqc_results" \
    --noextract \
    --threads $(($(nproc) / 2))

exit 0

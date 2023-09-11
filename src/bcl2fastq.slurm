#!/bin/bash
#SBATCH --job-name=gensvc-convert
#SBATCH --account=ISAAC-UTK0192
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=56
#SBATCH --exclusive=mcs
#SBATCH --partition=condo-ut-genomics
#SBATCH --time=0-08:00:00
#SBATCH --output=job-%J-%x.o
#SBATCH --qos=genomics
#SBATCH --mail-type=ALL
#SBATCH --mail-user=bioinformatics@utk.edu

# ulimit -n 16384

rundir="$1"
samplesheet="$2"
fastqdir="$3"

errors=0

# Check for arguments.
if [[ "$#" -ne 3 ]] ; then
    echo "Usage: bcl2fastq.slurm <rundir> <samplesheet> <fastqdir>"
    echo "  <rundir>       path to Illumina run (must already exist)"
    echo "  <samplesheet>  path to SampleSheet.csv (must already exist)"
    echo "  <rundir>       bcl2fastq will output fastq files into this directory (will be created)"
    exit 2
fi

# Check that rundir exists.
if [[ ! -d "$rundir" ]] ; then
    echo "[ERROR] rundir does not exist: $rundir"
    ((errors++))
else
    echo "[DEBUG] rundir      : $rundir"
fi

# Check that the sample sheet exists.
if [[ ! -f "$samplesheet" ]] ; then
    echo "[ERROR] samplesheet does not exist: $samplesheet"
    ((errors++))
else
    echo "[DEBUG] samplesheet : $samplesheet"
fi

# Make the fastq directory.
mkdir -v $fastqdir
if [[ $? -ne 0 ]] ; then
    echo "[ERROR] Failed to created fastqdir : $fastqdir"
    ((errors++))
else
    echo "[DEBUG] fastqdir    : $fastqdir"
fi

# Exit if there are errors above.
if [[ errors -ne 0 ]] ; then
    echo "[INFO] Exiting due to errors (see above)"
    exit 1
fi

source /usr/share/Modules/init/bash
module load bcl2fastq2

bcl2fastq \
    --processing-threads 48 \
    --runfolder-dir "$rundir" \
    --output-dir "$fastqdir" \
    --sample-sheet "$samplesheet"

exit 0

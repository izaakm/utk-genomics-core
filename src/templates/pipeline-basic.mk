#!/usr/bin/make -f

SHELL := /bin/bash

SLURM_ACCOUNT ?= acf-utk0011
SLURM_PARTITION ?= short
SLURM_QOS ?= short
SLURM_CPUS_PER_TASK ?= 16
SLURM_TIMELIMIT ?= 03:00:00

SRUN := srun \
        --nodes 1 \
        --ntasks 1 \
        --cpus-per-task $(SLURM_CPUS_PER_TASK) \
        --export=ALL,LC_ALL=C

ifdef SLURM_JOB_ID
SRUN += --exclusive
else
SRUN += --job-name qc-$(notdir $(PWD)) \
        --account $(SLURM_ACCOUNT) \
        --partition $(SLURM_PARTITION) \
        --qos $(SLURM_QOS) \
        --time $(SLURM_TIMELIMIT) \
        --mail-user \
        --mail-type=END,FAIL
endif

FASTQC := apptainer exec 'https://depot.galaxyproject.org/singularity/fastqc:0.12.1--hdfd78af_0' fastqc

FASTQ_FILES := $(wildcard fastq/*)
FASTQC_FILES := $(patsubst fastq/%.fastq.gz,fastqc/%.html,$(FASTQ_FILES))

.PHONY: all
all: checksums.sha256 $(FASTQC_FILES)

checksums.sha256: $(FASTQ_FILES)
	$(SRUN) bash -c 'find fastq -type f -name "*.fastq.gz" -print0 | xargs -0 -I {} -P0 sha256sum {} > $(@)'

.PHONY: fastqc
fastqc: $(FASTQC_FILES)
fastqc/%.html: fastq/%.fastq.gz
	mkdir -p fastqc
	$(SRUN) $(FASTQC) --threads $(SLURM_CPUS_PER_TASK) -o fastqc $(<)

#!/usr/bin/make -f

SHELL := /bin/bash

SLURM_ACCOUNT ?= acf-utk0011
SLURM_PARTITION ?= short
SLURM_QOS ?= short
CPUS_PER_TASK ?= 16

SRUN := srun \
		--account $(SLURM_ACCOUNT) \
		--partition $(SLURM_PARTITION) \
		--qos $(SLURM_QOS) \
		--ntasks 1 \
		--cpus-per-task $(CPUS_PER_TASK) \
		--export=ALL,LC_ALL=C \
		--time 03:00:00

ifdef SLURM_JOB_ID
SRUN += --exclusive
endif

FASTQC := apptainer exec 'https://depot.galaxyproject.org/singularity/fastqc:0.12.1--hdfd78af_0' fastqc

FASTQ_FILES := $(wildcard fastq/*)
FASTQC_FILES := $(patsubst fastq/%.fastq.gz,fastqc/%.html,$(FASTQ_FILES))

.PHONY: all
all: checksums.sha256 $(FASTQC_FILES)

checksums.sha256: $(FASTQ_FILES)
	$(SRUN) bash -c 'find $(^) -print0 | xargs -0 -I {} -P0 sha256sum {} > $(@)'

.PHONY: fastqc
fastqc: $(FASTQC_FILES)
fastqc/%.html: fastq/%.fastq.gz
	mkdir -p fastqc
	$(SRUN) $(FASTQC) --threads $(CPUS_PER_TASK) -o fastqc $(<)

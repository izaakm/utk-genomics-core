OIT HPSC Genomics Service
=========================

This repo contains a set of scripts for processing sequencing results from the Illumina MiSeq and NovaSeq instruments. These scripts do the following:

1. Check the raw data directories for new sequencing runs.
1. Create a new output directory for each sequencing run (see directory structure below).
1. Run `bcl2fastq` to convert raw BCL data to FASTQ sequence data.
1. Extract summary statistics as CSV files.


Data Directory structure
------------------------

[TODO] Update this

    gensvc/
        MiSeqRuns/                 # raw data directory
            <RUN_DIR>/
        NovaSeqRuns/               # raw data directory
            <RUN_DIR>/
        processed/                 # outputs will go here
            <RUNID>/
                <TIMESTAMP>/
                    <RUN_DIR>/     # symlink
                    <OUT_DIR>/     # directory
                    <SAMPLE_SHEET> # csv file
        logs/
            submit.log             # tracks submission of conversion job from src/launchconvert.sh
            convert.log            # tracks conversion jobs from src/convert.slurm


Overview of scripts
-------------------

`src/launchconvert2.sh`
: This script is intended to be run automatically via cron. It checks for new
  sequencing runs and submits conconversion jobs for new runs.
    
`src/convert.slurm`
: This is the primary script for converting sequencing data. It is a wrapper
  for all of the automated processing functions.

`src/extract-bcl2fastq-stats.py`
: This script extracts tables from the `bcl2fastq` summary statistics (html)
  and converts them to CSV files that can be shared with users.

More information about each specific script is available via the help `-h`
option:

```
<script> -h
```


Roadmap
-------

- [ ] [#1][] list all sequencing runs and current "status", which can be:
    - copy: complete
    - BCL Convert: {submitted,inprogress,complete}
    - transfer: complete
    - email confirmation: complete
    - `status` is prob part of the `core_facility` module and is completely
      separate from the `data.illumina` module because knowing the 'status' of
      a run depends on the data structure that the core facility uses
- [ ] [#2][] map user info to run
- [ ] [#3][] add `README` to transfer directories
- [ ] [#4][] create templates (jinja?) for per-run, per-project scripts (e.g., fastqc)
- [ ] [#5] ...


<!-- END -->

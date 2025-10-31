import os
import sys

from gensvc.data import illumina
from gensvc.core_facility.sequencing_run import SequencingRun

def cli(args):
    '''
    Command-line interface for managing SampleSheets.

    args.path_or_runid : str
        The path to the SampleSheet file or the run ID or path to the sequencing run.
    path_to_samplesheet
        The path to the SampleSheet file to create or update.
    seqrun
        The SequencingRun object associated with the SampleSheet.
    source
        The source samplesheet that will be updated or from which the new sample sheet will be created.
    '''
    if args.subcommand == "create":
        seqrun = None
        path_to_samplesheet = None
    elif args.subcommand == "update":
        if os.path.isdir(args.path_or_runid):
            # Assume it's a path to the sequencing run directory.
            runid = os.path.basename(args.path_or_runid)
            seqrun = SequencingRun(runid, path_to_rundir=args.path_or_runid)
            path_to_samplesheet = seqrun.rundir.path_to_samplesheet
        elif os.path.isfile(args.path_or_runid):
            # Assume it's a path to the SampleSheet file.
            path_to_samplesheet = args.path_or_runid
        else:
            # Assume it's a run ID.
            seqrun = SequencingRun(args.path_or_runid)
            path_to_samplesheet = seqrun.rundir.path_to_samplesheet
    elif args.subcommand == "validate":
        raise NotImplementedError('SampleSheet validation not yet implemented.')

    if not os.path.isfile(path_to_samplesheet):
        sys.tracebacklimit = 0
        raise FileNotFoundError(f'SampleSheet file not found: {path_to_samplesheet}')

    print(f'Path to SampleSheet: {path_to_samplesheet}')

    if args.src_sample_sheet:
        sample_sheet = illumina.read_sample_sheet(args.src_sample_sheet)
    elif path_to_samplesheet:
        sample_sheet = illumina.read_sample_sheet(path_to_samplesheet)
    elif args.format == 'v1':
        sample_sheet = illumina.SampleSheetv1()
    else:
        sample_sheet = illumina.SampleSheetv2()

    ############################################################
    # Data section
    ############################################################
    if args.check_duplicate_indexes:
        dupes = sample_sheet.duplicate_indexes()
        if dupes.empty:
            logger.info('No duplicate indexes found.')
        else:
            logger.info('Duplicate indexes found:')
            print(dupes.to_csv(index=False, sep='\t'))
            sys.tracebacklimit = 0
            raise ValueError('Duplicate indexes found in sample sheet.')

    if args.check_hamming_distances:
        # If the allowed "barcode mismatches" is 1, then the minimum hamming distance is 2, etc.
        if args.min_hamming_distance:
            min_hamming_distance = args.min_hamming_distance
        elif args.barcode_mismatches:
            min_hamming_distance = args.barcode_mismatches + 1
        else:
            raise ValueError('You must provide either --min-hamming-distance or --barcode-mismatches.')
        dlist = sample_sheet.hamming_distances()
        insufficient_hamming_distance = False
        for item in dlist:
            # item is a dict with keys: 'u', 'v', 'hamming', and (possibly) 'reverse_complement'
            # u: index1, v: index2, hamming: hamming distance, reverse_complement: 0=>u, 1=>v
            if item['hamming'] < min_hamming_distance:
                insufficient_hamming_distance = True
                # ---
                # [TODO] Add 'filter_indexes' method to SampleSheet class.
                # Takes and `index` and optional `index2` argument and return
                # samples (rows) with matching index.
                # >>> def filter_indexes(self, indexes, which='both', as_mask=False):
                # >>>     # indexes is a list of indexes to check.
                # >>>     if which == 'both':
                # >>>         <check passed indexes against both 'index' and 'index2' in the data>
                # >>>     elif which == 'index1':
                # >>>         <only check against index1>
                # >>>     elif which == 'index2':
                # >>>         <only check against index2>
                # ---
                # match_index1 = df['index'] == str(item['u'])
                # match_index2 = df['index2'] == str(item['v'])
                # print(f'{item["u"]} vs {item["v"]} hamming={item["hamming"]} (rc: {item.get("reverse_complement", None)})')
                # print(df[match_index1 | match_index2])
                # print()
                indexes = [item['u'], item['v']]
                df = sample_sheet.filter_sample_indexes(indexes, which='both', as_mask=False)
                # logger.info(f'{item["u"]} vs {item["v"]} hamming={item["hamming"]} (rc: {item.get("reverse_complement", None)})')
                # logger.info(df.to_csv(index=False, sep='\t'))
        if insufficient_hamming_distance:
            logger.warning('Insufficient Hamming distance found between some indexes. See above for details.')
            sys.exit(1)

    if args.project_suffix:
        if sample_sheet.Data.data.get('Sample_Project') is not None:
            # v1 sample sheets only have the 'Data' section. For v2 sample
            # sheets, 'Data' is aliased to 'BCLConvert_Data'.
            mapper = illumina.get_sample_project(
                sample_sheet.Data.data,
                project_col='Sample_Project'
            )
        elif sample_sheet.format == 'v2' and sample_sheet.Cloud_Data.data.get('ProjectName') is not None:
            # Only v2 sample sheets have 'Cloud_Data'.
            mapper = illumina.get_sample_project(
                sample_sheet.Cloud_Data.data,
                project_col='ProjectName'
            )
        else:
            sys.tracebacklimit = 0
            raise ValueError('Sample sheet does not have a "Sample_Project" or "ProjectName" column.')

        # Add the suffix to the project names.
        mapper = { k: f'{v}_{args.project_suffix}' for k, v in mapper.items() }

        if sample_sheet.Data.data.get('Sample_Project') is not None:
            illumina.set_sample_project(
                sample_sheet.Data.data,
                mapper,
                samples_col='Sample_ID',
                project_col='Sample_Project',
            )

        if sample_sheet.format == 'v2' and sample_sheet.Cloud_Data.data.get('ProjectName') is not None:
            illumina.set_sample_project(
                sample_sheet.Cloud_Data.data,
                mapper,
                samples_col='Sample_ID',
                project_col='ProjectName',
            )

    if args.projectname_to_sampleproject:
        if sample_sheet.format == 'v1':
            sys.tracebacklimit = 0
            raise ValueError('Project name to sample project mapping is only valid for v2 sample sheets.')
        # Only valid for V2 sample sheets.
        sample_sheet.projectname_to_sampleproject()

    if args.merge_duplicate_indexes:
        sample_sheet.merge_duplicate_indexes()
    
    ############################################################
    # Settings section
    ############################################################
    if args.create_fastq_for_index_reads:
        if sample_sheet.format == 'v1':
            sample_sheet.Settings.CreateFastqForIndexReads = 1
        elif sample_sheet.format == 'v2':
            sample_sheet.BCLConvert_Settings.CreateFastqForIndexReads = 1

    # Print to stdout
    if args.output == '-':
        print(sample_sheet.to_csv())
    elif path_to_samplesheet is None and not args.output:
        print(sample_sheet.to_csv())
    elif args.output:
        if os.path.exists(args.output) and not args.force:
            sys.tracebacklimit = 0
            raise FileExistsError(f'Output file already exists: {args.output} (use --force to overwrite)')
        # Write to file.
        print('[NOT IMPLEMENTED] Writing SampleSheet to:', args.output)
    elif path_to_samplesheet:
        if os.path.exists(path_to_samplesheet) and not args.force:
            sys.tracebacklimit = 0
            raise FileExistsError(f'Output file already exists: {path_to_samplesheet} (use --force to overwrite)')
        # Overwrite existing samplesheet.
        print('[NOT IMPLEMENTED] Overwriting SampleSheet at:', path_to_samplesheet)

    return 0

import pathlib
import re

def read_sample_sheet(path):
    # header = re.compile('^\[\s*Header\s*]')
    section = re.compile('^\[\s*(\w+)\s*]')
    
    sample_sheet = dict()
    lines = []
    key = None
    with open(path) as f:
        for line in f:
            sec = section.match(line)
            if sec:
                # New section
                # print(sec.group(0), sec.group(1))
                if key and lines:
                    sample_sheet[key] = lines

                key = sec.group(1)
                lines = []
            else:
                lines.append(line.strip())
        else:
            if key and lines:
                sample_sheet[key] = lines
    
    return sample_sheet

def looks_like_sample_sheet(path):
    if not isinstance(path, pathlib.Path):
        try:
            path = pathlib.Path(path)
        except:
            return False

    if not path.is_file():
        return False

    try:
        sample_sheet = read_sample_sheet(path)
    except:
        return False

    if sample_sheet.get('Header') and sample_sheet.get('Reads') and sample_sheet.get('Data'):
        return True
    else:
        return False

def find_samplesheet(dirname):
    '''
    Search a directory for an Illumina Sample Sheet file.
    '''
    found = []
    real = []

    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    for path in dirname.iterdir():
        if path.is_file():
            if looks_like_sample_sheet(path):
                found.append(path)
                real.append(path.resolve())

    if len(set(real)) == 1:
        match = real[0]
    else:
        match = None
    
    return match, dict(zip(found, real))

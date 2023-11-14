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

def looks_like_sample_sheet(sample_sheet):
    if isinstance(sample_sheet, (str, pathlib.Path)):
        sample_sheet = read_sample_sheet(sample_sheet)
    if sample_sheet.get('Header') and sample_sheet.get('Reads') and sample_sheet.get('Data'):
        return True
    else:
        return False

def find_samplesheet(dirname):
    if isinstance(dirname, str):
        dirname = pathlib.Path(dirname)
    found = []
    real = []

    for path in dirname.iterdir():
        if looks_like_sample_sheet(path):
            found.append(path)
            real.append(path.resolve())

    if len(set(real)) == 1:
        match = real[0]
    else:
        match = None
    
    return match, dict(zip(found, real))

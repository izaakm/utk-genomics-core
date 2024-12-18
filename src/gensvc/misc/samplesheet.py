import pathlib
import re

def read_sample_sheet(path):
    # header = re.compile(r'^\[\s*Header\s*]')
    section = re.compile(r'^\[\s*(\w+)\s*]')
    
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

def looks_like_samplesheet(path):
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
            if looks_like_samplesheet(path):
                found.append(path)
                real.append(path.resolve())

    # if len(set(real)) == 1:
    #     match = real[0]
    # else:
    #     match = None
    # return dict(zip(found, real))

    return sorted(real)

class SampleSheet:
    def __init__(self, path, data=None):
        self._path = path
        self._data = data

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self._path}>'

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = pathlib.Path(value)

    @property
    def realpath(self):
        return self._path.resolve()

    @property
    def data(self):
        if not self._data:
            self._data = read_sample_sheet(self.path)
        return self._data

    @data.setter
    def data(self, value):
        if isinstance(value, dict):
            self._data = value
        else:
            self._data = {}

import pathlib
import re
import csv

from io import StringIO


def csv_read(content):
    '''
    This is the preferred function for reading CSV data. This function and
    'csv_split' are similar in performance on small data sets, but 'csv_split'
    is really just included for comparison.
    '''
    if isinstance(content, list):
        content = '\n'.join(content)
    f = StringIO(content)
    reader = csv.reader(f)
    for row in reader:
        yield row


def csv_split(content):
    '''
    This function and 'csv_read' are similar in performance on small data sets,
    but 'csv_read' should be preferred because it uses 'csv.reader' from the
    standard lib, and is therefore expected to be more reliable.
    '''
    if isinstance(content, str):
        content = content.split('\n')
    for line in content:
        yield line.split(',')


def parse_header(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Header' section of an Illumina Sample Sheet.
    '''
    data = {}
    reader = csv_reader(list_of_lines)
    for row in reader:
        vals = [i for i in row if i]
        if not vals:
            continue
        elif len(vals) != 2:
            raise ValueError(f'Too many values: {vals!r}')
        else:
            data[vals[0]] = vals[1]
    return data


def parse_reads(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Reads' section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    data = []
    for row in reader:
        tmp = [int(i) for i in row if i]
        if tmp:
            data.append(tmp)
    return data


def parse_settings(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Settings' section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    data = []
    for row in reader:
        tmp = [i for i in row if i]
        if tmp:
            data.append(tmp)
    return data


def parse_data(list_of_lines, csv_reader=csv_read):
    '''
    Parse the 'Data' (samples) section of an Illumina Sample Sheet.
    '''
    reader = csv_reader(list_of_lines)
    header = next(reader)
    data = []
    for row in reader:
        data.append(dict(zip(header, row)))
    return data


def read_samplesheet(path):
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
    # print('Checking path ...')
    if not isinstance(path, pathlib.Path):
        try:
            path = pathlib.Path(path)
        except:
            # print('Cannot convert to path.')
            return False

    if not path.is_file():
        # print('Not a file.')
        return False

    try:
        sample_sheet = read_samplesheet(path)
    except:
        # print('Cannot read file.')
        return False

    if sample_sheet.get('Header') and sample_sheet.get('Reads'):
        # print('FOUND SAMPLE SHEET!!!')
        return True
    else:
        # print('Missing "Header" or "Reads"')
        return False

def find_samplesheet(dirname):
    '''
    Search a directory for an Illumina Sample Sheet file.

    [TODO] Sort multiple sample sheets by modified time.
    '''
    canonical = []
    real = []
    symlinks = []

    # print(dirname)
    if not isinstance(dirname, pathlib.Path):
        dirname = pathlib.Path(dirname)

    # print(dirname)
    for path in dirname.iterdir():
        # print(path)
        if path.is_file() and path.suffix == '.csv':
            if looks_like_samplesheet(path):
                path = path.absolute()
                if path.name == 'SampleSheet.csv':
                    canonical.append(path)
                elif path.is_symlink():
                    symlinks.append(path)
                else:
                    real.append(path)
    return canonical + real + symlinks


class SampleSheet:
    def __init__(self, path):
        self._path = path
        self._content = None
        self._header = None
        self._reads = None
        self._settings = None
        self._data = None

    def __repr__(self):
        return f'{self.__class__.__name__}("{self._path}")'

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
    def text(self):
        return self.path.read_text()

    @property
    def content(self):
        '''
        Helper for the other properties. Read the data and parse it into
        sections, but don't actually parse the sections yet.
        '''
        if not self._content:
            self._content = read_samplesheet(self.path)
        return self._content

    @property
    def sections(self):
        return [key for key in self.content.keys()]

    @property
    def Header(self):
        if not self._header:
            self._header = parse_header(self.content.get('Header', []))
        return self._header

    @property
    def Reads(self):
        if not self._reads:
            self._reads = parse_reads(self.content.get('Reads', []))
        return self._reads

    @property
    def Settings(self):
        if not self._settings:
            self._settings = parse_settings(self.content.get('Settings', []))
        return self._settings

    @property
    def Data(self):
        if not self._data:
            self._data = parse_data(self.content.get('Data', []))
        return self._data
    
    samples = Data

    @property
    def projects(self):
        return sorted(set([row.get('Sample_Project') for row in self.Data]))

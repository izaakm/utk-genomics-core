import pathlib


class Datadir():
    def __init__(self, path):
        self._path = pathlib.Path(path)

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.path}")'

    @property
    def path(self):
        return self._path

    @property
    def realpath(self):
        return self._path.resolve()

    @property
    def info(self):
        return self.__dict__



class RawData(Datadir):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class ProcessedData(Datadir):
    '''
    DEPRECATED.
    '''
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)



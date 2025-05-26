import pathlib
import tempfile
import logging
import sys

logger = logging.getLogger(__name__)

module_path = pathlib.Path(__file__).parent


def make_test_data(which, parent, parent_must_exist=True):
    '''
    '''
    if parent_must_exist and not parent.is_dir():
        sys.tracebacklimit = 0
        raise FileNotFoundError(f'Parent directory does not exist or is not a directory: {parent}')

    path_to_targets_list = module_path / 'datadir' / f'{which}.txt'
    destination = pathlib.Path(f'{parent}/{which}')
    with open(path_to_targets_list, 'r') as f:
        for line in f:
            target = destination / line.strip()
            logging.debug(f'Creating {target}')
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch(exist_ok=True)

    return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) != 3:
        logger.error('Usage: make_test_data.py <which> <parent>')
        sys.exit(1)
    which = sys.argv[1]
    parent = pathlib.Path(sys.argv[2])

    make_test_data(which, parent)

    sys.exit(0)

import logging

logger = logging.getLogger('root')

# Sets the correct format and only one logger ....
logging.basicConfig(
    format='[%(asctime)s] %(name)s %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# END

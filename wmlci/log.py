import logging
import shutil
import sys
from esupy.processed_data_mgmt import mkdir_if_missing
from wmlci.settings import logoutputpath

try:
    from colorama import init, Fore, Style
    init()
    class ColoredFormatter(logging.Formatter):
        FORMATS = {
            logging.DEBUG: logging.Formatter(
                Fore.CYAN + '%(asctime)s %(levelname)-8s' + Fore.RESET + ' %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'),
            logging.INFO: logging.Formatter(
                Fore.GREEN + '%(asctime)s %(levelname)-8s' + Fore.RESET + ' %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'),
            logging.WARNING: logging.Formatter(
                Fore.YELLOW + '%(asctime)s %(levelname)-8s' + Fore.RESET + ' %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'),
            logging.ERROR: logging.Formatter(
                Fore.RED + '%(asctime)s %(levelname)-8s' + Fore.RESET + ' %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'),
            logging.CRITICAL: logging.Formatter(
                Fore.RED + Style.BRIGHT + '%(asctime)s %(levelname)-8s' + Style.RESET_ALL + ' %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
        }

        def format(self, record):
            return self.FORMATS.get(record.levelno, self._fmt).format(record)
except ModuleNotFoundError:
    print("Install `colorama` for colored console logs")
    ColoredFormatter = logging.Formatter

file_formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S')

def get_log_file_handler(name='wmlci.log', level=logging.INFO):
    handler = logging.FileHandler(logoutputpath / name, mode='w', encoding='utf-8')
    handler.setLevel(level)
    handler.setFormatter(file_formatter)
    return handler

def setup_logger(name='wmlci_py', level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(ColoredFormatter())
    logger.addHandler(console_handler)

    file_handler = get_log_file_handler()
    logger.addHandler(file_handler)

    return logger

log = setup_logger()


def reset_log_file(filename, _meta):
    """
    Rename the log file saved to local directory using df meta and
        reset the log
    :param filename: str, name of dataset
    :param fb_meta: metadata for parquet
    """
    # original log file name - all log statements
    log_file = logoutputpath / "wmlci.log"
    # generate new log name
    new_log_name = (logoutputpath / f'{filename}_v'
                    f'{_meta.tool_version}'
                    f'{"_" + _meta.git_hash if _meta.git_hash else ""}'
                    f'.log')
    # create log directory if missing
    mkdir_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)

    # Reset log file
    for h in log.handlers:
        if isinstance(h, logging.FileHandler):
            log.removeHandler(h)
    log.addHandler(get_log_file_handler('wmlci.log', logging.INFO))

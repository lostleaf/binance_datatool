"""
QuantClass Simons' Log Kit
"""

import logging
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path

from colorama import Fore, Style, init

init(autoreset=True)

current_script = Path(sys.argv[0]).stem

# ====================================================================================================
# ** Add 'ok' log level **
# Add a custom log level to the default logging module
# Allowing logger.ok to output a success message.
# ====================================================================================================
OK_LEVEL = 25
logging.addLevelName(OK_LEVEL, "OK")


def ok(self, message, *args, **kwargs):
    if self.isEnabledFor(OK_LEVEL):
        self._log(OK_LEVEL, message, args, **kwargs)


logging.Logger.ok = ok


# ====================================================================================================
# ** Helper function **
# ====================================================================================================
def get_display_width(text: str) -> int:
    """
    Get the display width of text. 
    :param text: Input text
    :return: Display width of the text
    """
    width = 0
    for char in text:
        if unicodedata.east_asian_width(char) in ('F', 'W', 'A'):
            width += 1.685
        else:
            width += 1
    return int(width)


# ====================================================================================================
# ** Simons Log Tool **
# - SimonsFormatter(): Custom log formatter
# - SimonsConsoleHandler(): Custom console output
# - SimonsLogger(): Log utility
# ====================================================================================================
class SimonsFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: ('', ''),
        logging.INFO: (Fore.WHITE, "🌀 "),
        logging.WARNING: (Fore.YELLOW, "🔔 "),
        logging.ERROR: (Fore.RED, "❌ "),
        logging.CRITICAL: (Fore.RED + Style.BRIGHT, "⭕ "),
        OK_LEVEL: (Fore.GREEN, "✅ "),
    }

    def format(self, record):
        color, prefix = self.FORMATS.get(record.levelno, (Fore.WHITE, ""))
        record.msg = f"{color}{prefix}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


class SimonsConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        if record.levelno == logging.DEBUG:
            print(record.msg, flush=True)
        elif record.levelno == OK_LEVEL:
            super().emit(record)
        else:
            super().emit(record)


class SimonsLogger:
    _instance = dict()

    def __new__(cls, name='DataTool'):
        if cls._instance.get(name) is None:
            cls._instance[name] = super(SimonsLogger, cls).__new__(cls)
            cls._instance[name]._initialize_logger(name)
        return cls._instance[name]

    def _initialize_logger(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Clear handlers if any exist
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Add console output
        console_handler = SimonsConsoleHandler(sys.stdout)
        console_handler.setFormatter(SimonsFormatter("%(message)s"))
        self.logger.addHandler(console_handler)


# ====================================================================================================
# ** Utility functions **
# - get_logger(): Get a logger instance, optionally with a specific name
# - divider(): Draw a line with a timestamp
# - logger: Default logger object, can be used directly for standalone scripts
# ====================================================================================================
def get_logger(name=None):
    if name is None:
        name = current_script
    return SimonsLogger(name).logger


def divider(name='', sep='=', _logger=None, with_timestamp=True) -> None:
    """
    Draw a line with a timestamp
    :param name: Text in the middle
    :param sep: Separator character
    :return: No return value, directly draws a line
    :param _logger: Specific log file for output
    :param with_timestamp: Whether to include a timestamp
    """
    seperator_len = 82
    if with_timestamp:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        middle = f' {name} {now} '
    else:
        middle = f' {name} '
    middle_width = get_display_width(middle)
    decoration_count = max(4, (seperator_len - middle_width) // 2)
    line = sep * decoration_count + middle + sep * decoration_count

    # Add an extra separator if the total length is insufficient
    if get_display_width(line) < seperator_len:
        line += sep

    if _logger:
        _logger.debug(line)
    else:
        logger.debug(line)
    time.sleep(0.02)


logger = get_logger()

# Run directly to see usage examples
if __name__ == '__main__':
    # Output log information
    logger.debug("Debug information without markers or colors, equivalent to print")
    logger.info("Informational message in blue, useful for recording intermediate results")
    # noinspection PyUnresolvedReferences
    logger.ok("Completion message in green, typically indicating success")
    logger.warning("Warning message in yellow, typically used for alerts")
    logger.error("Error message in red, usually error-related hints")
    logger.critical("Critical message in dark red, typically very important information")
    divider('This is my divider function')
    divider('You can change the separator characters', sep='*')
    divider('The text is centered, and I’ve tried to adapt for both English and Chinese...', sep='-')

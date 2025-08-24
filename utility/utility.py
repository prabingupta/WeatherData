# /Users/prabinkumargupta/Downloads/WeatherData/ETL/utility/utility.py
import logging
import sys
from datetime import timedelta

def setup_logging(log_file_name: str) -> logging.Logger:
    """Setup logger with console + file handlers."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # clear duplicate handlers
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # file handler
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

def format_time(seconds: float) -> str:
    """Convert seconds to formatted h:m:s string."""
    td = timedelta(seconds=int(seconds))
    hh = int(seconds) // 3600
    mm = (int(seconds) % 3600) // 60
    ss = int(seconds) % 60
    return f"{hh:02d}h:{mm:02d}m:{ss:02d}s ({td})"

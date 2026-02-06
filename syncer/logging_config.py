import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    handler = RotatingFileHandler(
        "../sync.log",
        maxBytes=5*1024*1024,  # 5 MB на файл
        backupCount=3
    )
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

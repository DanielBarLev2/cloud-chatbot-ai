import logging, os

def get_logger(name: str):
    """
    Get a configured logger instance.
    Args:
        name (str): The name of the logger.
    Returns:
        logging.Logger: Configured logger instance.
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        h = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        h.setFormatter(fmt)
        logger.addHandler(h)
        logger.propagate = False
    return logger
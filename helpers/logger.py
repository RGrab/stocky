import sys
import logging


# Create and configure logger


def get_basic_logger(logger_level) -> logging:

    logger_level = str(logger_level).lower()

    match logger_level.lower():
        case 'info':
            level = logging.INFO
        case 'error':
            level = logging.ERROR
        case 'warning':
            level = logging.WARNING
        case 'debug':
            level = logging.DEBUG
        case _:
            get_basic_logger('warning').warning(f'could not find appropriate logger level for {logger_level}. setting '
                                                f'default level : info')
            level = logging.INFO

    log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')

    logger = logging.getLogger()

    # writing to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(log_format)
    logger.addHandler(handler)

    logger.setLevel(level)

    return logger

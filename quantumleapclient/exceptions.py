import logging


logger = logging.getLogger(__name__)


class QuantumLeapClientException(Exception):
    logger.debug("init QuantumLeapClientexception")

    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message
        logger.error(f'status:{status}|{message}')

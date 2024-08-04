import os
import hashlib

from util import get_logger

logger = get_logger()


def verify_checksum(data_path):
    checksum_path = data_path + '.CHECKSUM'
    if not os.path.exists(checksum_path):
        logger.error('Checksum file not exists %s', data_path)
        return False

    try:
        with open(checksum_path, 'r') as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except:
        logger.error('Error reading checksum file', checksum_path)
        return False

    with open(data_path, 'rb') as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()

    if checksum_value != checksum_standard:
        logger.error('Checksum error %s', data_path)
        return False

    return True

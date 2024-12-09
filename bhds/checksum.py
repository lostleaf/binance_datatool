import hashlib
from pathlib import Path

from util.log_kit import logger


def verify_checksum(data_path: Path):
    checksum_path = data_path.parent / (data_path.name + '.CHECKSUM')
    if not checksum_path.exists():
        return False, 'Checksum file not exists'

    try:
        with open(checksum_path, 'r') as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except:
        return False, 'Error reading checksum file'

    with open(data_path, 'rb') as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()

    if checksum_value != checksum_standard:
        return False, 'Checksum not equal'

    return True, None

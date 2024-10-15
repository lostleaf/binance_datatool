import os
import hashlib

from joblib import Parallel, delayed

from config import Config
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


def run_verify_checksum(paths):
    logger = get_logger()
    unverified_paths = []

    for p in paths:
        verify_file = p + '.verified'
        if not os.path.exists(verify_file):
            unverified_paths.append(p)

    logger.info('%d files to be verified', len(unverified_paths))

    results = Parallel(Config.N_JOBS)(delayed(verify_checksum)(p) for p in paths)

    for unverified_path, verify_success in zip(unverified_paths, results):
        if verify_success:
            with open(unverified_path + '.verified', 'w') as fout:
                fout.write('')
        else:
            logger.warning('%s failed to verify, deleting', unverified_path)
            if os.path.exists(unverified_path):
                os.remove(unverified_path)
            checksum_path = unverified_path + '.CHECKSUM'
            if os.path.exists(checksum_path):
                os.remove(checksum_path)

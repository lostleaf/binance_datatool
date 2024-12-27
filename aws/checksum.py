import hashlib
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import config
from util.concurrent import mp_env_init
from util.log_kit import logger


def get_checksum_file(data_file: Path):
    checksum_file = data_file.parent / (data_file.name + '.CHECKSUM')
    return checksum_file


def get_verified_file(data_file: Path):
    verified_file = data_file.parent / (data_file.name + '.verified')
    return verified_file


def verify_checksum(data_file: Path):
    checksum_path = get_checksum_file(data_file)
    if not checksum_path.exists():
        return False, 'Checksum file not exists'

    try:
        with open(checksum_path, 'r') as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except:
        return False, 'Error reading checksum file'

    with open(data_file, 'rb') as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()

    if checksum_value != checksum_standard:
        return False, 'Checksum not equal'

    return True, None


def verify_aws_data_file(data_file: Path):
    is_success, error = verify_checksum(data_file)

    if not is_success:
        logger.error(f'{error}, file={data_file}')
        checksum_file = get_checksum_file(data_file)

        data_file.unlink(missing_ok=True)
        checksum_file.unlink(missing_ok=True)
        return False

    verified_file = get_verified_file(data_file)
    verified_file.touch()
    return True


def get_unverified_aws_data_files(symbol_dir: Path) -> list[Path]:
    unverified_files = []
    for kline_file in symbol_dir.glob('*.zip'):
        verify_file = get_verified_file(kline_file)
        if not verify_file.exists():
            unverified_files.append(kline_file)

    return unverified_files


def get_verified_aws_data_files(symbol_dir: Path) -> list[Path]:
    verified_files = []
    for kline_file in symbol_dir.glob('*.zip'):
        verify_file = get_verified_file(kline_file)
        if verify_file.exists():
            verified_files.append(kline_file)

    return verified_files


def verify_multi_process(unverified_files):
    num_success, num_fail = 0, 0
    with ProcessPoolExecutor(max_workers=config.N_JOBS, mp_context=mp.get_context('spawn'),
                             initializer=mp_env_init) as exe:
        tasks = [exe.submit(verify_aws_data_file, kline_file) for kline_file in unverified_files]
        for task in as_completed(tasks):
            is_success = task.result()
            if is_success:
                num_success += 1
            else:
                num_fail += 1
    return num_success, num_fail

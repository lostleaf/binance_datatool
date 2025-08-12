import hashlib
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm



def get_checksum_file(data_file: Path):
    checksum_file = data_file.parent / (data_file.name + ".CHECKSUM")
    return checksum_file


def get_verified_file(data_file: Path):
    verified_file = data_file.parent / (data_file.name + ".verified")
    return verified_file


class AwsDataFileManager:
    def __init__(self, base_dir: Path, delete_mismatch: bool = False):
        self.base_dir = base_dir
        self.delete_mismatch = delete_mismatch

    def get_files(self) -> list[Path]:
        verified_files, unverified_files = [], []
        for kline_file in self.base_dir.glob("*.zip"):
            verify_file = get_verified_file(kline_file)
            if verify_file.exists():
                verified_files.append(kline_file)
            else:
                unverified_files.append(kline_file)
        return verified_files, unverified_files

    def get_verified_files(self) -> list[Path]:
        verified_files, _ = self.get_files()
        return verified_files

    def get_unverified_files(self) -> list[Path]:
        _, unverified_files = self.get_files()
        return unverified_files


def calc_checksum(data_file: Path):
    with open(data_file, "rb") as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()
    return checksum_value


def verify_checksum(data_file: Path):
    checksum_path = get_checksum_file(data_file)
    if not checksum_path.exists():
        return False, "Checksum file not exists"

    try:
        with open(checksum_path, "r") as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except:
        return False, "Error reading checksum file"

    checksum_value = calc_checksum(data_file)

    if checksum_value != checksum_standard:
        return False, "Checksum not equal"

    return True, None


def delete_data_file(data_file: Path, with_checksum: bool):
    data_file.unlink(missing_ok=True)

    verified_file = get_verified_file(data_file)
    verified_file.unlink(missing_ok=True)

    if with_checksum:
        checksum_file = get_checksum_file(data_file)
        checksum_file.unlink(missing_ok=True)


def verify_data_file(data_file: Path, delete_mismatch: bool = False):
    is_success, error = verify_checksum(data_file)

    # Checksum matched
    if is_success:
        verified_file = get_verified_file(data_file)
        verified_file.touch()
        return True, data_file, None

    # Checksum not matched
    if delete_mismatch:
        delete_data_file(data_file, with_checksum=True)
    return False, data_file, error


def verify_multi_process(aws_managers: list[AwsDataFileManager], delete_mismatch: bool, n_jobs: int):
    num_success, num_fail = 0, 0
    errors = dict()

    all_unverified_files = []
    for mgr in aws_managers:
        unverified_files = mgr.get_unverified_files()
        all_unverified_files.extend(unverified_files)

    # No polars involved
    with tqdm(total=len(all_unverified_files), desc="Verify data", unit="task") as pbar:
        with ProcessPoolExecutor(max_workers=n_jobs) as exe:
            tasks = [exe.submit(verify_data_file, kline_file, delete_mismatch) for kline_file in all_unverified_files]
            for task in as_completed(tasks):
                is_success, data_file, error = task.result()
                pbar.update(1)
                pbar.set_postfix({"file": data_file.name})

                if is_success:
                    num_success += 1
                else:
                    num_fail += 1
                    errors[data_file] = error
    return num_success, num_fail, errors

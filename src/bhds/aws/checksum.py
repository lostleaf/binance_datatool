import hashlib
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from tqdm import tqdm


def get_checksum_file(data_file: Path) -> Path:
    """
    Get the path to the checksum file corresponding to a data file.
    
    Args:
        data_file: Path to the data file
        
    Returns:
        Path to the .CHECKSUM file in the same directory as the data file
    """
    checksum_file = data_file.parent / (data_file.name + ".CHECKSUM")
    return checksum_file


def get_verified_file(data_file: Path) -> Path:
    """
    Get the path to the verification mark file corresponding to a data file.
        
    Args:
        data_file: Path to the data file
        
    Returns:
        Path to the .verified file in the same directory as the data file
    """
    verified_file = data_file.parent / (data_file.name + '.verified')
    return verified_file


def calc_checksum(data_file: Path) -> str:
    """
    Calculate SHA256 checksum of the file by reading the file content and computing its SHA256 hash.
    
    Args:
        data_file: Path to the file to calculate checksum for
        
    Returns:
        SHA256 checksum as a hexadecimal string
    """
    with open(data_file, "rb") as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()
    return checksum_value


def read_checksum(checksum_path: Path) -> str:
    """
    Read checksum value from checksum file

    Args:
        checksum_path: Path to the checksum file

    Returns:
        Checksum value
    """
    if not checksum_path.exists():
        raise FileNotFoundError(f"Checksum file {checksum_path} not exists")

    try:
        with open(checksum_path, "r") as fin:
            text = fin.read()
        checksum_standard = text.strip()
        return checksum_standard
    except Exception as e:
        raise RuntimeError(f"Checksum Error {checksum_path}: {e}")


class ChecksumVerifier:
    """Checksum verifier for validating AWS data file integrity"""

    def __init__(self, delete_mismatch: bool = False, n_jobs: Optional[int] = None):
        """
        Initialize checksum verifier

        Args:
            delete_mismatch: Whether to delete file if verification fails
            n_jobs: Number of parallel processes, defaults to CPU cores - 2
        """
        self.delete_mismatch = delete_mismatch
        self.n_jobs = n_jobs or max(1, mp.cpu_count() - 2)

    def verify_file(self, data_file: Path) -> bool:
        """
        Verify checksum of a single file

        Args:
            data_file: Path to the data file to verify

        Returns:
            Success flag
        """
        checksum_path = get_checksum_file(data_file)
        checksum_standard = read_checksum(checksum_path)

        checksum_value = calc_checksum(data_file)

        if checksum_value != checksum_standard:
            if self.delete_mismatch:
                self._cleanup_files(data_file)
            return False

        # Create verification mark
        verified_file = get_verified_file(data_file)
        verified_file.touch()
        return True

    def verify_files(self, files: list[Path]) -> dict:
        """
        Batch verify files

        Args:
            files: List of files to verify

        Returns:
            Dictionary containing verification results
        """
        results = {"success": 0, "failed": 0, "errors": {}, "total_files": len(files)}

        if not files:
            return results

        with tqdm(total=len(files), desc="Verifying files", unit="file") as pbar:
            with ProcessPoolExecutor(max_workers=self.n_jobs) as executor:
                future_to_file = {executor.submit(self.verify_file, f): f for f in files}

                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["errors"][file_path] = "Checksum mismatch"
                    except Exception as e:
                        results["failed"] += 1
                        results["errors"][file_path] = str(e)

                    pbar.update(1)
                    pbar.set_postfix({"success": results["success"], "failed": results["failed"]})

        return results

    def _cleanup_files(self, data_file: Path) -> None:
        """
        Cleanup files after verification failure

        Args:
            data_file: Path to the data file that failed verification
        """
        data_file.unlink(missing_ok=True)

        verified_file = get_verified_file(data_file)
        verified_file.unlink(missing_ok=True)

        checksum_file = get_checksum_file(data_file)
        checksum_file.unlink(missing_ok=True)


class AwsDataFileManager:
    """
    Manager for AWS data files that handles file verification status tracking.
    
    This class provides utilities to manage and categorize AWS data files based on
    their verification status using .verified marker files.
    """
    
    def __init__(self, base_dir: Path):
        """
        Initialize the AWS data file manager.
        
        Args:
            base_dir: Base directory containing the AWS data files
        """
        self.base_dir = base_dir

    def get_files(self) -> tuple[list[Path], list[Path]]:
        """
        Get both verified and unverified ZIP files from the base directory.
        
        Scans the base directory for .zip files and categorizes them based on the corresponding .verified marker file.
        
        Returns:
            Tuple of (verified_files, unverified_files) where:
            - verified_files: List of .zip files that have .verified markers
            - unverified_files: List of .zip files without .verified markers
        """
        verified_files, unverified_files = [], []
        for kline_file in self.base_dir.glob("*.zip"):
            verify_file = get_verified_file(kline_file)
            if verify_file.exists():
                verified_files.append(kline_file)
            else:
                unverified_files.append(kline_file)
        return verified_files, unverified_files

    def get_verified_files(self) -> list[Path]:
        """
        Get only the verified ZIP files from the base directory.
        
        Returns:
            List of .zip files that have been successfully verified
        """
        verified_files, _ = self.get_files()
        return verified_files

    def get_unverified_files(self) -> list[Path]:
        """
        Get only the unverified ZIP files from the base directory.
        
        Returns:
            List of .zip files that have not been verified yet
        """
        _, unverified_files = self.get_files()
        return unverified_files

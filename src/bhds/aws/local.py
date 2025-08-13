"""
Local file management utilities for AWS data files.

This module provides utilities for managing local AWS data files, including
verification status tracking and file categorization.
"""

from pathlib import Path
from typing import List, Tuple

from bhds.aws.checksum import get_verified_file


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

    def get_files(self) -> Tuple[List[Path], List[Path]]:
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

    def get_verified_files(self) -> List[Path]:
        """
        Get only the verified ZIP files from the base directory.
        
        Returns:
            List of .zip files that have been successfully verified
        """
        verified_files, _ = self.get_files()
        return verified_files

    def get_unverified_files(self) -> List[Path]:
        """
        Get only the unverified ZIP files from the base directory.
        
        Returns:
            List of .zip files that have not been verified yet
        """
        _, unverified_files = self.get_files()
        return unverified_files
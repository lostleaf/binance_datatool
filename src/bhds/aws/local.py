"""
Local file management utilities for AWS data files.

This module provides utilities for managing local AWS data files, including
verification status tracking and file categorization.
"""

from pathlib import Path
from typing import Dict, List, Tuple

from bhds.aws.checksum import get_verified_file
from bhds.aws.path_builder import AwsPathBuilder


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


class LocalAwsClient:
    """
    Local AWS client for managing downloaded Binance historical data files.
    
    This client provides methods similar to AwsClient but operates on local
    downloaded files, allowing you to list symbols and their verification status.
    """
    
    def __init__(self, base_dir: Path, path_builder: AwsPathBuilder):
        """
        Initialize local AWS client.
        
        Args:
            base_dir: Base directory containing downloaded AWS data files
            path_builder: AWS path builder for constructing directory paths
        """
        self.base_dir = Path(base_dir)
        self.path_builder = path_builder
    
    def get_symbol_dir(self, symbol: str) -> Path:
        """
        Get the local directory path for a specific symbol.
        
        Args:
            symbol: Trading symbol (e.g., BTCUSDT)
            
        Returns:
            Path object representing the local symbol directory path
        """
        relative_path = self.path_builder.get_symbol_dir(symbol)
        return self.base_dir / relative_path
    
    def list_symbols(self) -> List[str]:
        """
        List all available symbols in the local directory.
        
        Returns:
            Sorted list of symbol names as strings
        """
        base_path = self.base_dir / self.path_builder.base_dir
        if not base_path.exists():
            return []
        
        symbols = []
        for item in base_path.iterdir():
            if item.is_dir():
                symbols.append(item.name)
        
        return sorted(symbols)
    
    def list_data_files(self, symbol: str) -> List[Path]:
        """
        List all data files for a specific symbol.
        
        Args:
            symbol: Trading symbol to list files for
            
        Returns:
            List of Path objects representing data file paths
        """
        symbol_dir = self.get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return []
        
        files = []
        for file_path in symbol_dir.glob("*.zip"):
            files.append(file_path)
        
        return sorted(files)
    
    def get_symbol_file_status(self, symbol: str) -> Dict[str, List[Path]]:
        """
        Get file verification status for a specific symbol.
        
        Args:
            symbol: Trading symbol to check files for
            
        Returns:
            Dictionary with 'verified' and 'unverified' keys containing lists of file paths
        """
        symbol_dir = self.get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return {"verified": [], "unverified": []}
        
        manager = AwsDataFileManager(symbol_dir)
        verified_files = manager.get_verified_files()
        unverified_files = manager.get_unverified_files()
        
        return {
            "verified": verified_files,
            "unverified": unverified_files
        }
    
    def batch_get_symbol_file_status(self, symbols: List[str]) -> Dict[str, Dict[str, List[Path]]]:
        """
        Get file verification status for multiple symbols.
        
        Args:
            symbols: List of trading symbols to check files for
            
        Returns:
            Dictionary mapping symbol names to their file status dictionaries
        """
        result = {}
        for symbol in symbols:
            result[symbol] = self.get_symbol_file_status(symbol)
        return result
    
    def get_all_symbols_status(self) -> Dict[str, Dict[str, List[Path]]]:
        """
        Get file verification status for all available symbols.
        
        Returns:
            Dictionary mapping symbol names to their file status dictionaries
        """
        symbols = self.list_symbols()
        return self.batch_get_symbol_file_status(symbols)
    
    def get_summary(self) -> Dict[str, int]:
        """
        Get a summary of all symbols and their file counts.
        
        Returns:
            Dictionary with summary statistics including total symbols,
            total files, verified files, and unverified files
        """
        symbols = self.list_symbols()
        total_verified = 0
        total_unverified = 0
        total_files = 0
        for symbol in symbols:
            status = self.get_symbol_file_status(symbol)
            verified_count = len(status["verified"])
            unverified_count = len(status["unverified"])
            total_verified += verified_count
            total_unverified += unverified_count
            total_files += verified_count + unverified_count
        
        return {
            "total_symbols": len(symbols),
            "total_files": total_files,
            "verified_files": total_verified,
            "unverified_files": total_unverified
        }
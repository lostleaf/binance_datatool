"""Test utilities for the binance data tool project

This module provides common utility functions used across test files.
"""

from pathlib import Path


def output_directory_structure(
    directory: Path,
    max_depth: int = 3,
    current_depth: int = 0,
    max_files_per_dir: int = 5,
):
    """Print directory structure with samples (limited files per directory)

    Args:
        directory: Directory path to print structure for
        max_depth: Maximum depth to traverse
        current_depth: Current traversal depth (used internally)
        max_files_per_dir: Maximum number of files to show per directory
    """
    if current_depth >= max_depth:
        return

    items = sorted(directory.iterdir())
    dirs = [item for item in items if item.is_dir()]
    files = [item for item in items if item.is_file()]

    # Print directories first
    for item in dirs:
        indent = "  " * current_depth
        print(f"{indent}{item.name}/")
        output_directory_structure(item, max_depth, current_depth + 1, max_files_per_dir)

    # Print limited number of files
    for i, item in enumerate(files):
        if i >= max_files_per_dir:
            indent = "  " * current_depth
            print(f"{indent}... ({len(files) - max_files_per_dir} more files)")
            break
        indent = "  " * current_depth
        print(f"{indent}{item.name}")

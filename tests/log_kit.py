"""
Test file for log_kit module usage examples
"""

from bdt_common.log_kit import logger, divider


def test_log_kit_examples():
    """Run log_kit usage examples to see different log levels and divider function"""
    # Output log information
    logger.debug("Debug information without markers or colors, equivalent to print")
    logger.info("Informational message in blue, useful for recording intermediate results")
    # noinspection PyUnresolvedReferences
    logger.ok("Completion message in green, typically indicating success")
    logger.warning("Warning message in yellow, typically used for alerts")
    logger.error("Error message in red, usually error-related hints")
    logger.critical("Critical message in dark red, typically very important information")
    divider("This is my divider function")
    divider("You can change the separator characters", sep="*")
    divider("The text is centered, and I've tried to adapt for both English and Chinese...", sep="-")


if __name__ == "__main__":
    test_log_kit_examples()
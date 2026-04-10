"""Shared constants for the binance_datatool package."""

# Base URL for S3-compatible HTTP listing of the Binance public data archive.
S3_LISTING_PREFIX = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

# Default timeout in seconds for a single HTTP request to the S3 listing endpoint.
S3_HTTP_TIMEOUT_SECONDS = 15

#!/bin/bash

# Backup script for Binance AWS data
# Backs up $CRYPTO_BASE_DIR/binance_data/aws_data directory
# Excludes .verified files and creates compressed backup

set -e

# Determine CRYPTO_BASE_DIR from environment or default to $HOME/crypto_data
CRYPTO_BASE_DIR=${CRYPTO_BASE_DIR:-$HOME/crypto_data}
SOURCE_DIR="$CRYPTO_BASE_DIR/binance_data/aws_data"
BACKUP_DIR="$CRYPTO_BASE_DIR/binance_data"

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

# Create backup filename with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_NAME="aws_data_backup_${TIMESTAMP}.tar"
BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"

echo "Starting backup of AWS data..."
echo "Source: $SOURCE_DIR"
echo "Backup: $BACKUP_PATH"

# Create backup archive excluding .verified files
tar -cf "$BACKUP_PATH" \
    --exclude='*.verified' \
    -C "$BACKUP_DIR" \
    aws_data

# Calculate backup size
BACKUP_SIZE=$(du -h "$BACKUP_PATH" | cut -f1)

echo "Backup completed successfully!"
echo "Archive: $BACKUP_PATH"
echo "Size: $BACKUP_SIZE"

# List backup file details
ls -lh "$BACKUP_PATH"
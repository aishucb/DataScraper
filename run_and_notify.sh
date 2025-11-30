#!/bin/bash

# Paths
SOURCE="/var/www/DataScraper/data/daily/"
RCLONE="/usr/bin/rclone"
CONFIG="/home/ubuntu/.config/rclone/rclone.conf"
REMOTE="scraper:daily-backup"

# Check if CSV files exist
FILES=$(ls $SOURCE/*.csv 2>/dev/null | wc -l)

if [ "$FILES" -eq 0 ]; then
    # No CSV files found
    curl -X POST https://api.twilio.com/2010-04-01/Accounts/AC67cffc84b0ffca0cb95e91604a4f13f8/Messages.json \
        --data-urlencode "To=whatsapp:+919656554244" \
        --data-urlencode "From=whatsapp:+14155238886" \
        --data-urlencode "Body=No CSV files found. Nothing to upload." \
        -u AC67cffc84b0ffca0cb95e91604a4f13f8:da74f025d5742ae039d8dcc532c5dff0
    exit 0
fi

# Run rclone move
sudo $RCLONE --config $CONFIG move "$SOURCE" "$REMOTE" \
  --include "*.csv" --transfers=4 --checkers=8 --progress

STATUS=$?

if [ $STATUS -eq 0 ]; then
    # Success
    curl -X POST https://api.twilio.com/2010-04-01/Accounts/AC67cffc84b0ffca0cb95e91604a4f13f8/Messages.json \
        --data-urlencode "To=whatsapp:+919656554244" \
        --data-urlencode "From=whatsapp:+14155238886" \
        --data-urlencode "Body=CSV files moved successfully to Google Drive." \
        -u AC67cffc84b0ffca0cb95e91604a4f13f8:da74f025d5742ae039d8dcc532c5dff0
else
    # Failed
    curl -X POST https://api.twilio.com/2010-04-01/Accounts/AC67cffc84b0ffca0cb95e91604a4f13f8/Messages.json \
        --data-urlencode "To=whatsapp:+919656554244" \
        --data-urlencode "From=whatsapp:+14155238886" \
        --data-urlencode "Body=Rclone upload FAILED. Please check the server." \
        -u AC67cffc84b0ffca0cb95e91604a4f13f8:da74f025d5742ae039d8dcc532c5dff0
fi

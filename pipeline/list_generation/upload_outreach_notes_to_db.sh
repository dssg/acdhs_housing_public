#!/bin/bash

echo "Loading the environmental variables"
export $(xargs < .env)

# name of the target dir, ideally use the date e.g. 20241113 
ZIPNAME=$1

# unzip file
cd /mnt/data/projects/acdhs_housing/outreach_spreadsheets/saved_notes/
unzip Current_spreadsheets.zip -d $ZIPNAME # files are extracted into directory $ZIPNAME/Current_spreadsheets/
mv $ZIPNAME/Current_spreadsheets/* $ZIPNAME/
rmdir $ZIPNAME/Current_spreadsheets/

cd /mnt/data/users/alice/acdhs_housing/
python src/pipeline/postmodeling/upload_outreach_notes_to_db.py -d $ZIPNAME

rm /mnt/data/projects/acdhs_housing/outreach_spreadsheets/saved_notes/Current_spreadsheets.zip
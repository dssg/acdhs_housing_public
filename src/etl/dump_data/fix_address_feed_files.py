"""
The dataset we received from ACDHS with addresses (address_feed) contains CSVs with special characters that prevent us from copying to the DB straight away. 
This script fixes the rows with such problems. 
"""

import csv
import os
import logging


def _files_need_fixing(folder_name):
    return [x for x in os.listdir(folder_name) if '_tofix' in x]


def fix_address_csv(file_path):
    """clean a given csv file
    
        1. remove "" chars in the file and use \t as the delimiter, make sure only 7 fields are in the CSV
        2. Add one column with the complete address 
    """

    logging.info('Fixinf file -- {}'.format(file_path))
    rows = []

    with open(file_path, 'r', errors='ignore') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            l = [row[0], row[1].replace('"', ''), row[2].replace('"', '')] + row[-4:]
            # appending the full address
            l.append(','.join(row[1:]))
            rows.append(l)
            
    new_pth = file_path[:-9] + 'fixed.csv'
    with open(new_pth, 'w', newline='\n') as csvfile:
        wrt = csv.writer(csvfile, delimiter='\t', quotechar='|')

        for row in rows:
            if len(row) > 8:
                print('Not fixed')
            wrt.writerow(row)


def run(folder_path):
    """run the cleaning pipeline"""

    fs_fixing = _files_need_fixing(folder_path)

    for fname in fs_fixing:
        pth = os.path.join(folder_path, fname)

        fix_address_csv(pth)


if __name__ == '__main__':
    folder = '/mnt/data/projects/acdhs-housing/data/20220606-snap/address_feed_csvs/modified'

    run(folder)

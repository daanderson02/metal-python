import shutil
import os

# really basic script to backup the full database and create a new one
# this new one is the one that's actually targeted in genre cleaner

# check if ./backups exists
os.makedirs('backups',exist_ok=True) 

# define the original and new file paths
original_file = 'full database.xlsx'
new_file = './backups/full database backup.xlsx'

# copy and rename the file
shutil.copyfile(original_file, new_file)

print(f"file '{original_file}' has been copied and renamed to '{new_file}'")

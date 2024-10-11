import pandas as pd
import glob
import os

# combines all separate csv files into one and transforms to xlsx. used to
# compare the completeness and accuracy to the full database sheet exported
# by scraper versus the individual database sheets. also for quality of life
# fixes the year formed column which for some reason just hates me

# file name
xlsx_file = 'full database.xlsx'

# define the path to csv files within folder
csv_files = glob.glob('./data/*.csv')

# combine all files into a single dataframe
combined_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# converts years to numbers because thats what they are yknow?
combined_df['Year Formed'] = pd.to_numeric(combined_df['Year Formed'], errors='coerce')

# gets rid of decimals for consistency
combined_df['Year Formed'] = combined_df['Year Formed'].fillna(0).astype(int)

# replace zeros with "Unknown"
combined_df['Year Formed'] = combined_df['Year Formed'].replace(0, 'Unknown')

# export dataframe to excel spreadsheet, pushes it one folder up, and combines
# all disparate dataframes into one single sheet
combined_df.to_excel(xlsx_file, index=False, sheet_name='data')

print(f"output combined into '{xlsx_file}'")

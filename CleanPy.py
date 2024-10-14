import pandas as pd
import re
from tqdm import tqdm

# script to clean genres up to make them easier to visualize in data visualization programs.
# does a few things. first, it searches for instances of (later) or (early)  or (mid). 
# if it finds this, it finds the later genre and keeps only that. then it iterates through
# keywords_map with a simple for loop and replaces the string with whatever genre comes first.
# also has some special compounded keywords. finally just saves it out to xlsx

# load the original database spreadsheet
file_path = 'full database.xlsx'
sheet_name = 'data' 
genre_column = 'Genre'  

# read the Excel file
df = pd.read_excel(file_path, sheet_name=sheet_name)

# initialize tqdm progress bar for cleaning genres. not terribly useful as it goes pretty quick
tqdm.pandas(desc="Cleaning Genres")

# define keywords to be replaced
keywords_map = {
    # compounded keywords should go first

    'atmospheric, black': 'Atmospheric Black Metal',
    'ambient, black': 'Atmospheric Black Metal',
    'black, thrash': 'Thrash Metal',
    'melodic, death': 'Melodic Death Metal',
    'brutal, death': 'Brutal Death Metal',
    'technical, death': 'Technical Death Metal',
    'symphonic, black': 'Black Metal',
    'experimental, metal': 'Experimental Metal',
    'avant, garde, metal': 'Experimental Metal',
    'grind, roll': 'Grindcore',
    'progressive, death': 'Death Metal',
    'progressive, thrash': 'Thrash Metal',
    'progressive, black': 'Black Metal',
    'progressive, heavy': 'Heavy Metal',

    # regular keywords go second

    'depressive suicidal': 'Black Metal',
    'funeral': 'Funeral Doom Metal',
    'slam': 'Brutal Death Metal',
    'black': 'Black Metal',
    'thrash': 'Thrash Metal',
    'death': 'Death Metal',
    'doom': 'Doom Metal',
    'sludge': 'Sludge Metal',
    'progressive': 'Progressive Metal',
    'gothic': 'Gothic Metal',
    'deathcore': 'Metalcore',
    'metalcore': 'Metalcore',
    'speed': 'Speed Metal',
    'shred': 'Heavy Metal',
    'heavy': 'Heavy Metal',
    'power': 'Power Metal',
    'groove': 'Heavy Metal',
    'blackened folk': 'Black Metal',
    'black/folk': 'Black Metal',
    'folk/black': 'Black Metal',
    'folk': 'Folk Metal',
    'dungeon synth': 'Ambient',
    'grindcore': 'Grindcore',
    'goregrind': 'Grindcore',
    'powerviolence': 'Grindcore',
    'mathcore': 'Metalcore',
    'djent': 'Djent',
    'd-beat': 'Punk',
    'synthwave': 'Ambient',
    'nwobhm': 'Heavy Metal',
    'blues': 'Hard Rock',
    'crossover': 'Thrash Metal',
    'crust': 'Black Metal',
    'grunge': 'Hard Rock',
    'industrial': 'Industrial Metal',
    'nu-metal': 'Nu-Metal',
    'aor': 'Hard Rock',
    'stoner': 'Doom Metal',
    'pagan': 'Black Metal',
    'war': 'Black Metal',
    'neoclassical': 'Heavy Metal',
    'darkwave': 'Ambient',
	'alternative': 'Alternative Metal',
    'symphonic': 'Symphonic Metal',
    'post-metal': 'Post-Metal',
    'drone': 'Ambient',
    'viking': 'Folk Metal',
    'glam': 'Hard Rock',
    'rock': 'Hard Rock',
    'hardcore': 'Punk',
    'rac': 'Hard Rock',
    'noise': 'Ambient',
    'punk': 'Punk',
    'southern': 'Heavy Metal',
    'various': 'Various',
    'electronic': 'Ambient',
    'neofolk': 'Neofolk',
    'ambient': 'Ambient',

    # add more as needed
}

# function to clean genres based on combined keywords
def replace_genres(genre_string):
    genre_string = genre_string.lower().strip()
    genre_string = re.sub(r'[\/,]', ' ', genre_string)
    genre_string = re.sub(r'\s+', ' ', genre_string).strip()  # Normalize whitespace

    # for loop through keywords_map
    for combined_keywords, replacement in keywords_map.items():
        # split keywords into individual terms
        keyword_list = [kw.strip().lower() for kw in combined_keywords.split(',')]

        # check if all keywords are present in the genre string
        if all(re.search(r'\b' + re.escape(keyword) + r'\b', genre_string) for keyword in keyword_list):
            return replacement  

    return 'Undefined'  # returns undefined if no keywords match

# extract later genres
def extract_later_genres(genre_str):
    # find all instances of "(later)" or similar variations and extract the genres preceding them
    matches = re.findall(r'([A-Za-z\s]+)\s?\((?:early\/later|later|mid)\)', genre_str)
    if matches:
        # join and return the cleaned genre names
        return ', '.join(set(matches))
    else:
        return genre_str 

# later genres ONLY
df['Genre'] = df['Genre'].apply(extract_later_genres)

# good genres ONLY
df[genre_column] = df[genre_column].progress_apply(replace_genres)

# save changes ONLY
df.to_excel(file_path, sheet_name=sheet_name, index=False)

print(f"processing complete. updated file saved to {file_path}")

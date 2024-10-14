# Metal Python
 
a set of scripts meant to scrape, combine, and clean data received from metal-archives.com. collects the following stuff (see MetalPy.py for further info): 

                'Band Name', 'Country', 'Genre', 'Themes', 'Band Status', 'Year Formed', 'Label',
                'Number of Reviews', 'Number of EPs', 'Number of Full-Lengths',
                'Number of Demos', 'Average Review Score', 'Min Review Score', 'Max Review Score', 'Added By', 'Added On', 'Band ID'

some code i used chatgpt for because i was getting obscenely frustrated. most AI stuff was in metalpy itself because i have not made a scraper before. and got peeved off a few times. also some code in cleanpy for def extract_later_genres. 

everything else mine. i dont care about licenses if this helps you just use it i guess. made for class ACC430 Data Analytics for Financial Professionals. hi professor.

general use case: 
- step 1: download python scripts or clone repository or something and download required python stuff
- step 2a: edit (if wanted) the procedures of the scrape. you can edit what letters you want scraped, how many bands, etc
- step 2b: run MetalPy.py. i recommend running it on a device you're not using as it can take a while. i ran it on my Steam Deck
- step 3: cross reference errors in ./errors/ with bands on full_database.xlsx to make sure all bands were recorded properly
- step 4: run BackupPy.py. copies full_database.xlsx to ./backups/
- step 5: run CleanPy.py. cleans up genre names to give you about 30 unique genres rather than one quatrillion "Technical Avant-Garde Brutal Slam Djent Death Metal"
- step 6: idk lol run it through a BI program to visualize the cool stuff?

kthxbye

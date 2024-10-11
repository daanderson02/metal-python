import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import datetime
from tqdm import tqdm
import os

# this script is the main bread and butter. this script does a lot of things.
# its main shtick is that it slurps up metal archives bands, gets their details
# and then drills down into the discography using their id. contains several
# ways to avoid potential 429s and also to not hammer the metal archives
# servers. because thats just not cool. this script took me about five or so
# days to run uninterrupted. if you want to run this script yourself i
# highly recommend running it on a pc you don't use. i used my Steam Deck.
# anyway lol okay bye haha

os.makedirs('data',exist_ok=True)
os.makedirs('errors',exist_ok=True)

# url for metal archives plus variables for band ids and names
base_url = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1"
band_url_template = "https://www.metal-archives.com/band/view/id/{}"

# among us reference?!?? pretend browser imposter when not?? wtf?!
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0",
    "Accept-Language": "en-US,en;q=0.5",
}

# tracks time taken, bands scraped, and bytes sent and received
start_time = time.time()
total_bands_scraped = 0
total_bytes_sent = 0
total_bytes_received = 0
debug_log = []  # to store issues encountered during scraping. also exported to individual txt files on scraped letters

# debug variable. set band id to whatever you want to verify the script works. set to none to disable
debug_band_id = None

# set of letters to scrape
letters_to_scrape = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")  # includes ONLY letters

# add the special cases explicitly. metal archives uses NBR to represent numbers so we have to specially indicate that. also ~
special_cases = ["NBR", "~"]  # NBR for numbers and ~ for symbols and non-Latin alphabet

# dealing with 429s
MAX_RETRIES = 5
INITIAL_BACKOFF_WAIT = 10  # initial wait for backoff
BACKOFF_MULTIPLIER = 1.25  # multiplier for backoff
RESET_BACKOFF_TIME = 120  # time after multiplied backoff resets
backoff_wait_time = INITIAL_BACKOFF_WAIT  # current backoff wait time
last_429_time = None  # time when the last 429 was received

# maximum number of bands to scrape. set to like 999999999 or whatever if you want to scrape everything
max_bands_to_scrape = 999999999

# random delay between scrapes to mimic human behavior and lower server load
DELAY_BETWEEN_REQUESTS = (1, 5)  # random delay in seconds
PAUSE_PROBABILITY = 0.1  # percent chance to pause randomly
PAUSE_DURATION_RANGE = (1, 4)  # random pause duration in seconds

# function to clean genre text
def clean_genre_text(genre):
    return genre.replace(';', ',').strip()

# function to handle rate limiting
def handle_rate_limiting(response, url):
    global backoff_wait_time, last_429_time

    if response.status_code == 429:
        # update 429 time
        last_429_time = time.time()

        # wait time notification output into cli
        print(f"Banhammered! waiting 10 seconds before retrying URL: {url}")
        time.sleep(10)

        # retry after the initial wait
        retry_response = requests.get(url, headers=headers, timeout=10)
        if retry_response.status_code == 429:
            # still rate limited, apply multiplicative backoff
            retry_after = backoff_wait_time * random.uniform(1.5, 2.5)
            backoff_wait_time = min(retry_after, RESET_BACKOFF_TIME * 4)  # Cap wait to 4 times the reset period
            print(f"Uh oh! Another 429! Waiting {retry_after:.2f} seconds (multiplicative backoff).")
            time.sleep(retry_after)
            backoff_wait_time *= BACKOFF_MULTIPLIER  # increase backoff time
            return True  # indicate that we are still rate-limited
        else:
            print("successfully passed rate limit after initial wait, good job")
            backoff_wait_time = INITIAL_BACKOFF_WAIT  # reset backoff wait time
            return False
    return False

# reset backoff if enough time has passed without a 429
def reset_backoff():
    global backoff_wait_time
    if last_429_time and time.time() - last_429_time > RESET_BACKOFF_TIME:
        backoff_wait_time = INITIAL_BACKOFF_WAIT

# introduce random fluctuation in scraping so we seem human (among us reference)
def fluctuate_scraping_speed():
    global DELAY_BETWEEN_REQUESTS
    # random chance to pause
    if random.random() < PAUSE_PROBABILITY:
        pause_time = random.uniform(*PAUSE_DURATION_RANGE)
        print(f"lul random pause for {pause_time:.2f} seconds.")
        time.sleep(pause_time)
    else:
        # randomly adjust scraping speed (increase or decrease delay)
        DELAY_BETWEEN_REQUESTS = (
            max(1, DELAY_BETWEEN_REQUESTS[0] + random.uniform(-0.5, 0.5)),
            max(1, DELAY_BETWEEN_REQUESTS[1] + random.uniform(-0.5, 0.5))
        )

# function to extract basic band details with retry logic
def get_band_details(band_url):
    global total_bytes_sent, total_bytes_received
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(band_url, headers=headers, timeout=10)
            total_bytes_sent += len(response.request.body or '')
            total_bytes_received += len(response.content)

            if handle_rate_limiting(response, band_url):
                retries += 1
                continue

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # extract label information
                label_field = soup.find('dt', string=re.compile(r'(Current|Last known|Last) label:', re.IGNORECASE))
                label = label_field.find_next('dd').text.strip() if label_field else "Unknown"

                # extract year formed
                year_field = soup.find('dt', string="Formed in:")
                year_formed = year_field.find_next('dd').text.strip() if year_field else "Unknown"

                # extract lyrical themes
                themes_field = soup.find('dt', string="Themes:")
                themes = themes_field.find_next('dd').text.strip() if themes_field else "Unknown"

                # extract "added by" and "added on" from the bottom section of the page
                added_info_div = soup.find('div', id='auditTrail')

                added_by = "Unknown"
                added_on = "Unknown"

                if added_info_div:
                    added_by_element = added_info_div.find('a')
                    if added_by_element:
                        added_by = added_by_element.text.strip()

                    added_on_element = added_info_div.find(string=re.compile("Added on:"))
                    if added_on_element:
                        # extract date after "Added on:"
                        added_on = added_on_element.split("Added on:")[1].strip()

                return label, year_formed, themes, added_by, added_on

            # log all unexpected responses to debug log
            debug_log.append(f"unexpected response status code {response.status_code} for URL {band_url}")

        except Exception as e:
            debug_log.append(f"error fetching details for {band_url}: {e}")
            retries += 1
            time.sleep(random.uniform(1, 3))
    return "Unknown", "Unknown", "Unknown", "N/A", "Unknown"

# extract release counts and review information using discography
def get_discography_details(band_id):
    global total_bytes_sent, total_bytes_received
    retries = 0
    while retries < MAX_RETRIES:
        try:
            discography_url = f"https://www.metal-archives.com/band/discography/id/{band_id}/tab/all"
            response = requests.get(discography_url, headers=headers, timeout=10)
            total_bytes_sent += len(response.request.body or '')
            total_bytes_received += len(response.content)

            if handle_rate_limiting(response, discography_url):
                retries += 1
                continue

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # release counts
                num_eps = len(soup.find_all(string=re.compile(r"\bEP\b", re.IGNORECASE)))
                num_full_lengths = len(soup.find_all(string=re.compile(r"\bFull-length\b", re.IGNORECASE)))
                num_demos = len(soup.find_all(string=re.compile(r"\bDemo\b", re.IGNORECASE)))

                # sum review counts and average review amounts
                total_reviews = 0
                total_score = 0
                min_score = 100  # Setting high initial min_score so any actual score will be lower
                max_score = 0  # Setting low initial max_score so any actual score will be higher

                # find correct discography rows
                discography_rows = soup.find_all('tr')
                for row in discography_rows:
                    # extract review count and average score for each album
                    review_info = row.get_text()
                    match = re.search(r"(\d+)\s*\(\s*(\d+)%\s*\)", review_info)
                    if match:
                        review_count = int(match.group(1))
                        review_score = int(match.group(2))
                        total_reviews += review_count
                        total_score += review_count * review_score  # Weighted sum for calculating average

                        # track the min and max scores
                        min_score = min(min_score, review_score)
                        max_score = max(max_score, review_score)

                # if no reviews are found, set min and max to "N/A"
                min_score = min_score if total_reviews > 0 else "N/A"
                max_score = max_score if total_reviews > 0 else "N/A"

                # calculate weighted average review score using total reviews as a weight
                avg_review_score = round(total_score / total_reviews, 2) if total_reviews > 0 else "N/A"

                return num_eps, num_full_lengths, num_demos, total_reviews, avg_review_score, min_score, max_score

            debug_log.append(f"Unexpected response status code {response.status_code} for discography URL {discography_url}")

        except Exception as e:
            debug_log.append(f"Error fetching discography details for {discography_url}: {e}")
            retries += 1
            time.sleep(random.uniform(1, 3))
    return 0, 0, 0, 0, "N/A", "N/A", "N/A"

# extract the band ID from the band's page URL
def extract_band_id(band_url):
    try:
        band_id_match = re.search(r"/(\d+)$", band_url)
        return band_id_match.group(1) if band_id_match else None
    except Exception as e:
        debug_log.append(f"Error extracting band ID from {band_url}: {e}")
        return None

# get the total number of bands for a specific letter or special case
def get_total_bands_count(letter, retries=3, wait_time=5):
    global total_bytes_sent, total_bytes_received
    for attempt in range(retries):
        try:
            url = base_url.format(letter)
            debug_log.append(f"Attempting to fetch total bands for letter {letter} (Attempt {attempt + 1})")
            response = requests.get(url, headers=headers, timeout=10)
            total_bytes_sent += len(response.request.body or '')
            total_bytes_received += len(response.content)

            if handle_rate_limiting(response, url):
                retries += 1
                continue

            if response.status_code == 200:
                data = response.json()
                return data['iTotalRecords']
            else:
                debug_log.append(f"Unexpected status code {response.status_code} when fetching total bands for {letter}")

        except Exception as e:
            debug_log.append(f"Error getting total bands count for letter {letter}: {e}")

        # wait before retrying
        debug_log.append(f"Waiting {wait_time} seconds before retrying...")
        time.sleep(wait_time)

    # if all retries fail, return 0 and log the failure
    debug_log.append(f"Failed to fetch total bands count for letter {letter} after {retries} attempts")
    return 0

# function to scrape bands for a specific letter or special case
def scrape_bands_for_letter(letter):
    global total_bands_scraped, total_bytes_sent, total_bytes_received
    bands = []
    page = 0

    # get the total count of bands for letter (letter)
    total_bands = get_total_bands_count(letter)
    if total_bands == 0:
        debug_log.append(f"no bands found for letter {letter}. skipping")
        return bands

    # use tqdm to approximate time remaining. breaks itself really easily lul
    progress_bar = tqdm(total=total_bands, desc=f"Scraping {letter}", unit="band")

    while len(bands) < total_bands and total_bands_scraped < max_bands_to_scrape:
        try:
            # send a request for the next page of bands for the letter
            url = base_url.format(letter)
            params = {
                'sEcho': 1,
                'iColumns': 4,
                'iDisplayStart': page * 500,
                'iDisplayLength': 500,
                'mDataProp_0': '0',
                'mDataProp_1': '1',
                'mDataProp_2': '2',
                'mDataProp_3': '3'
            }
            response = requests.get(url, params=params, headers=headers, timeout=10)
            total_bytes_sent += len(response.request.body or '')
            total_bytes_received += len(response.content)

            if handle_rate_limiting(response, url):
                continue

            # check for other response issues
            if response.status_code != 200:
                debug_log.append(f"Failed to fetch data for letter {letter} (Status Code: {response.status_code})")
                break

            # parse the JSON response
            try:
                data = response.json()
                rows = data['aaData']
            except ValueError as e:
                debug_log.append(f"Error decoding JSON for letter {letter}: {e}")
                break

            if not rows:
                break

            # extract band information
            for row in rows:
                if total_bands_scraped >= max_bands_to_scrape:
                    break

                try:
                    # parse band name and URL
                    band_name_html = BeautifulSoup(row[0], 'html.parser')
                    band_name = band_name_html.find('a').text  # Safely extract band name
                    band_url = band_name_html.find('a')['href']  # Extract URL to the band's page

                    # extract band ID
                    band_id = extract_band_id(band_url)
                    if not band_id:
                        continue

                    # parse other fields
                    country = row[1]
                    genre = clean_genre_text(row[2])  # Clean genre to replace semicolons

                    # clean the band status 
                    status_html = BeautifulSoup(row[3], 'html.parser')
                    band_status = status_html.get_text() 

                    # get basic details
                    label, year_formed, themes, added_by, added_on = get_band_details(band_url)

                    # get releases, review average score, and min/max review scores. visits discography page for ease of parsing
                    num_eps, num_full_lengths, num_demos, num_reviews, avg_review_score, min_score, max_score = get_discography_details(band_id)

                    # add it to the list!
                    bands.append([
                        band_name, country, genre, themes, band_status, year_formed, label, num_reviews,
                        num_eps, num_full_lengths, num_demos, avg_review_score, min_score, max_score, added_by, added_on, band_id
                    ])

                    # update progress on tqdm
                    progress_bar.update(1)
                    total_bands_scraped += 1

                    # fluctuate scraping speed after each band to prevent breaking metal archives' servers
                    fluctuate_scraping_speed()

                except (IndexError, AttributeError) as e:
                    debug_log.append(f"error parsing row data for band URL {band_url}: {e}")
                    continue

            page += 1

            # randomized delay between requests. see above
            delay = random.uniform(*DELAY_BETWEEN_REQUESTS) 
            time.sleep(delay)

        except requests.exceptions.Timeout as e:
            debug_log.append(f"timeout while fetching letter {letter}, page {page}. Retrying...: {e}")
        except Exception as e:
            debug_log.append(f"error fetching band list for letter {letter}: {e}")

    progress_bar.close()
    return bands

# Function to save a summary for each letter
def save_letter_summary(letter, bands_scraped):
    letter_summary_file = f'./errors/{letter}_scraping_summary.txt'
    with open(letter_summary_file, 'w') as summary_file:
        summary_file.write(f"letter: {letter}\n")
        summary_file.write(f"bands Scraped: {bands_scraped}\n")
        summary_file.write(f"total Bytes Received: {total_bytes_received}\n")
        summary_file.write("debug Log:\n")
        for log in debug_log:
            summary_file.write(log + "\n")
    print(f"summary for letter {letter} saved to {letter_summary_file}")

# main scraping loop for all letters
def scrape_all_letters():
    global total_bands_scraped
    all_bands = []

    # scrape letters first
    for letter in letters_to_scrape:
        print(f"scraping bands for letter {letter}...")
        debug_log.clear()  # Clear debug log for each letter

        # scrape bands for this letter
        bands = scrape_bands_for_letter(letter)
        bands_scraped = len(bands)

        if bands_scraped > 0:
            # save the letter's data to csv file (located in data folder)
            csv_file_path = f'./data/{letter} scrape.csv'
            df = pd.DataFrame(bands, columns=[
                'Band Name', 'Country', 'Genre', 'Theme', 'Band Status', 'Year Formed', 'Label',
                'Number of Reviews', 'Number of EPs', 'Number of Full-Lengths',
                'Number of Demos', 'Average Review Score', 'Min Review Score', 'Max Review Score', 'Added By', 'Added On', 'Band ID'
            ])
            df.to_csv(csv_file_path, index=False, encoding='utf-8', sep=',')
            print(f"data for letter {letter} saved to {csv_file_path}")

            all_bands.extend(bands)

        # save a summary for the letter
        save_letter_summary(letter, bands_scraped)

        # stop if we've scraped the maximum number of bands
        if total_bands_scraped >= max_bands_to_scrape:
            break

    # scrape special cases
    for special_case in special_cases:
        print(f"scraping bands for special case '{special_case}'...")
        debug_log.clear()  # clear debug log for each special case

        # scrape bands for this special case
        bands = scrape_bands_for_letter(special_case)
        bands_scraped = len(bands)

        if bands_scraped > 0:
            # save the special case data to its own CSV file
            csv_file_path = f'{special_case} scrape.csv'
            df = pd.DataFrame(bands, columns=[
                'Band Name', 'Country', 'Genre', 'Themes', 'Band Status', 'Year Formed', 'Label',
                'Number of Reviews', 'Number of EPs', 'Number of Full-Lengths',
                'Number of Demos', 'Average Review Score', 'Min Review Score', 'Max Review Score', 'Added By', 'Added On', 'Band ID'
            ])
            df.to_csv(csv_file_path, index=False, encoding='utf-8', sep=',')
            print(f"Data for special case '{special_case}' saved to {csv_file_path}")

            all_bands.extend(bands)

        # save a summary for special cases
        save_letter_summary(special_case, bands_scraped)

        # stop if we've scraped the maximum number of bands
        if total_bands_scraped >= max_bands_to_scrape:
            break

    # export the complete dataframe to a CSV file
    full_xlsx_file_path = 'full database.xlsx'
    df_all = pd.DataFrame(all_bands, columns=[
        'Band Name', 'Country', 'Genre', 'Themes', 'Band Status', 'Year Formed', 'Label',
        'Number of Reviews', 'Number of EPs', 'Number of Full-Lengths',
        'Number of Demos', 'Average Review Score', 'Min Review Score', 'Max Review Score', 'Added By', 'Added On', 'Band ID'
    ])
    df_all.to_excel(full_xlsx_file_path, index=False)

    print(f"scraping completed and data saved to {full_xlsx_file_path}")

# decide whether to run in debug mode for a single band or scrape all letters. stupid code if i remove this it breaks and i DONT KNOW WHY
if debug_band_id is not None:
    scrape_band_by_id(debug_band_id)
else:
    scrape_all_letters()

# calculate total time taken
end_time = time.time()
time_taken = end_time - start_time
time_taken_str = str(datetime.timedelta(seconds=time_taken))

# save overall summary
completion_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
summary_file_path = 'overall_scraping_summary.txt'
with open(summary_file_path, 'w') as summary_file:
    summary_file.write(f"time taken to complete the script: {time_taken_str}\n")
    summary_file.write(f"total number of bands scraped: {total_bands_scraped}\n")
        summary_file.write(f"total bytes received: {total_bytes_received}\n")
    summary_file.write(f"date and time completed: {completion_datetime}\n")

print(f"summary saved to {summary_file_path}")

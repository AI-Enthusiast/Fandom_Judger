"""
Archive of Our Own (AO3) web scraper for the Fandom Judger project.
This script scrapes fanfiction metadata and content from AO3 for analysis.
"""
import time
import output_cleanup
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import random
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_wait_time(quick=True):
    """
    Generate a random wait time to avoid overwhelming the server.
    
    Args:
        quick (bool): If True, uses shorter wait times (19-31 seconds),
                     if False, uses longer wait times (30-60 seconds)
                     
    Returns:
        float: Random wait time in seconds
    """
    if quick:
        return random.uniform(8, 29)
    else:
        return random.uniform(30, 60)


def get_story_info(work_id, attempt=1):
    """
    Scrape detailed information about a specific AO3 work.
    
    This function extracts comprehensive metadata and content from an AO3 work page,
    including title, author, summary, tags, statistics, and the full story text.
    It handles various edge cases like restricted content, missing data, and rate limiting.
    
    Args:
        work_id (int): The unique AO3 work ID
        attempt (int): Current attempt number for retry logic
        
    Returns:
        tuple: (story_metadata_list, story_text) if successful, None if failed
               story_metadata_list contains: [work_id, title, author, summary, notes, 
               rating, warnings, categories, fandoms, relationships, characters,
               additional_tags, language, published, word_count, chapter_count,
               comment_count, kudos_count, bookmarks_count, hits_count]
    """
    # Construct the full URL with parameters to view full work and adult content
    url_prefix = 'https://archiveofourown.org/works/'
    url_suffix = '?view_full_work=true'
    adult_suffix = '&view_adult=true'
    url = url_prefix + str(work_id) + url_suffix + adult_suffix
    
    # Make the HTTP request
    raw = requests.get(url)
    bad_count = 0
    
    # Check for failed requests or login redirects
    if raw.status_code != 200 or raw.url == 'https://archiveofourown.org/users/login?restricted=true':
        return None
    
    soup = bs(raw.text, 'html.parser')
    
    # Check for rate limiting (retry later message)
    if soup.find('body').find('pre'):
        # print('Retry later hit for work_id:', work_id)
        # wait for 5 minutes
        if attempt < 1:
            time.sleep(300)
            return get_story_info(work_id, attempt=attempt + 1)
        else:
            return None
    
    # Check for 404 errors (deleted/private works)
    if soup.find('div', class_='system errors error-404 region'):
        return None

    # Extract basic metadata
    warnings = soup.find('dd', class_='warning tags').text
    language = soup.find('dd', class_='language').text

    # Clean up extracted text
    warnings = warnings.replace('\n\n', '')
    language = language.replace('\n', '')

    # Skip underage content and non-English works
    if warnings == 'Underage' or language != 'English':
        story = ''
    else:
        try:
            # Extract the full story text
            story = soup.find('div', id='chapters').find('div', class_='userstuff').text
        except:
            story = ''
            # print('Error with work_id:', work_id)
            bad_count += 1
    
    # Extract title with error handling
    try:
        title = soup.find('h2', class_='title heading').text
    except:
        title = None
        # print('Error with work_id:', work_id)
        bad_count += 1
    
    # Extract author with error handling
    try:
        author = soup.find('h3', class_='byline heading').text
    except:
        author = None
        # print('Error with work_id:', work_id)
        bad_count += 1
    
    # If too many critical fields are missing, skip this work
    if bad_count > 2:
        print('Error with work_id:', work_id)
        return None
    
    # Extract optional metadata fields
    try:
        summary = soup.find('div', class_='summary module').text
    except:
        summary = None
    
    try:  # not all stories have notes
        notes = soup.find('div', class_='notes module').text
    except AttributeError:
        notes = None
    
    # Extract required metadata
    rating = soup.find('dd', class_='rating tags').text

    try:
        categories = soup.find('dd', class_='category tags').text
    except:
        categories = None
    
    fandoms = soup.find('dd', class_='fandom tags').text
    
    try:
        relationships = soup.find('dd', class_='relationship tags').text
    except AttributeError:
        relationships = None
    
    # Extract character tags as a list
    try:
        characters = soup.find('dd', class_='character tags')
        characters = characters.find_all('a')
        characters = [character.text for character in characters]
    except:
        characters = None
    
    # Extract additional tags as a list
    try:
        additional_tags = soup.find('dd', class_='freeform tags')
        additional_tags = additional_tags.find_all('a')
        additional_tags = [tag.text for tag in additional_tags]
    except:
        additional_tags = None
    
    # Extract statistics
    stats = soup.find('dl', class_='stats')
    published = stats.find('dd', class_='published').text
    word_count = stats.find('dd', class_='words').text
    chapter_count = stats.find('dd', class_='chapters').text
    
    # Extract engagement metrics (may be missing for unpopular works)
    try:  # only if it's popular enough to have comments
        comment_count = stats.find('dd', class_='comments').text
    except AttributeError:
        comment_count = 0
    try:
        kudos_count = stats.find('dd', class_='kudos').text
    except AttributeError:
        kudos_count = 0
    try:
        bookmarks_count = stats.find('dd', class_='bookmarks').text
    except AttributeError:
        bookmarks_count = 0
    try:
        hits_count = stats.find('dd', class_='hits').text
    except AttributeError:
        hits_count = 0

    return [work_id, title, author, summary, notes, rating, warnings, categories, fandoms, relationships, characters,
            additional_tags, language, published, word_count, chapter_count, comment_count, kudos_count,
            bookmarks_count, hits_count], story


def process_work_id(work_id):
    """
    Process a single work ID by scraping its information and saving the story text.
    
    This function handles the complete workflow for a single work:
    1. Scrapes the work's metadata and content
    2. Saves the story text to a file
    3. Returns the metadata for database storage
    
    Args:
        work_id (int): The AO3 work ID to process
        
    Returns:
        list: Story metadata list, or list with mostly None values if failed
    """
    story_info = get_story_info(work_id)
    if story_info:
        # Save the story text to a file named with the work_id
        with open('output/stories/' + str(work_id) + '.txt', 'w') as f:
            f.write(story_info[1])
        return story_info[0]  # Return the metadata
    else:
        # Return a list with None values if the scraping failed
        return [work_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None]


def multi_scrape_ids(work_ids):
    """
    Scrape multiple work IDs concurrently using thread pool execution.
    
    This function processes multiple work IDs in parallel to improve scraping efficiency
    while respecting rate limits through individual request delays.
    
    Args:
        work_ids (list): List of AO3 work IDs to scrape
        
    Returns:
        list: List of story metadata lists from all processed works
    """
    story_db = []
    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor() as executor:
        # Submit all work_ids for processing
        futures = {executor.submit(process_work_id, work_id): work_id for work_id in work_ids}
        # Collect results as they complete
        # for future in tqdm(as_completed(futures), total=len(work_ids), desc="Scrapping works"):
        for future in as_completed(futures):
            story_db.append(future.result())
    return story_db


def get_work_ids(url, soup=None):
    """
    Extract work IDs from an AO3 search results page.
    
    This function parses an AO3 search page to extract all work IDs
    from the search results, which can then be used for detailed scraping.
    
    Args:
        url (str): URL of the AO3 search page
        soup (BeautifulSoup, optional): Pre-parsed soup object to avoid re-requesting
        
    Returns:
        list: List of work ID strings extracted from the page
    """
    if soup is None:
        raw = requests.get(url)
        soup = bs(raw.text, 'html.parser')
    
    # Find all work elements on the page
    works = soup.find_all('li', class_='work')
    # Extract work IDs from the href attributes
    work_ids = [work.find('a')['href'].split('/')[-1] for work in works]
    return work_ids


if __name__ == '__main__':
    """
    Main execution block for the AO3 scraper.
    
    This script performs a comprehensive scraping operation:
    1. Cleans up existing files and database entries
    2. Scrapes AO3 works sorted by popularity (hits)
    3. Saves both metadata and story content
    4. Handles pagination and rate limiting
    5. Performs final cleanup
    """
    
    # Commented out: Random scraping with different seeds
    # for i in range(10):
    #     rand_scrape(seed=42+i, n=5000)
    #     wait_time = 30*(i+1)
    #     print(f'Waiting for {wait_time} seconds')
    #     time.sleep(wait_time)
    
    # Clean up existing files before starting
    try:
        output_cleanup.clean_files()
    except FileNotFoundError:
        print('No existing files to clean up, starting fresh.')
    
    # AO3 search URL for all works sorted by hits (popularity) in descending order
    all_works_url = 'https://archiveofourown.org/works/search?work_search%5Bquery%5D=&work_search%5Btitle%5D=&work_search%5Bcreators%5D=&work_search%5Brevised_at%5D=&work_search%5Bcomplete%5D=&work_search%5Bcrossover%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bword_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bfandom_names%5D=&work_search%5Brating_ids%5D=&work_search%5Bcharacter_names%5D=&work_search%5Brelationship_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Bcomments_count%5D=&work_search%5Bbookmarks_count%5D=&work_search%5Bsort_column%5D=hits&work_search%5Bsort_direction%5D=desc&commit=Search'
    
    # URL components for pagination
    page_prefix = 'https://archiveofourown.org/works/search?commit=Search&page='
    page_suffix = '&work_search[bookmarks_count]=&work_search[character_names]=&work_search[comments_count]=&work_search[complete]=&work_search[creators]=&work_search[crossover]=&work_search[fandom_names]=&work_search[freeform_names]=&work_search[hits]=&work_search[kudos_count]=&work_search[language_id]=&work_search[query]=&work_search[rating_ids]=&work_search[relationship_names]=&work_search[revised_at]=&work_search[single_chapter]=0&work_search[sort_column]=hits&work_search[sort_direction]=desc&work_search[title]=&work_search[word_count]='
    
    # Pagination setup
    page = 2
    START = 750 + 1040  # Resume from this page number
    if START > page:
        page = START
    first = True
    
    # Get the total number of works available
    raw = requests.get(all_works_url)
    soup = bs(raw.text, 'html.parser')
    num_works = soup.find('h3', class_='heading').text
    num_works = num_works.split(' ')[0]
    num_works_int = int(num_works.replace(',', ''))
    num_of_pages = num_works_int // 20 + 1  # AO3 shows 20 works per page
    
    # Main scraping loop through all pages
    for i in tqdm(range(num_of_pages), desc='Scrapping pages', total=num_of_pages - START):
        # for i in range(num_of_pages):
        attempts = 0
        work_ids = []
        
        # Retry logic for getting work IDs (up to 3 attempts)
        while len(work_ids) == 0 and attempts < 3:
            if attempts > 0:
                time.sleep(300)  # Wait 5 minutes between retry attempts
            
            if first:
                # Use the pre-loaded soup for the first page
                work_ids = get_work_ids(all_works_url, soup)
                first = False
            else:
                # Generate random page number (exploration strategy)
                random_page = random.randint(1, num_of_pages)
                work_ids = get_work_ids(page_prefix + str(page) + page_suffix)
                page += 1
            attempts += 1
        
        # Break if no work IDs found after retries
        if len(work_ids) == 0:
            print('No work_ids found')
            break

        # Scrape all works from the current page
        story_db = multi_scrape_ids(work_ids)

        # Save the scraped data to the database
        if len(story_db) != 0:
            try:
                # Load existing database and append new data
                story_db = pd.DataFrame(story_db,
                                        columns=['work_id', 'title', 'author', 'summary', 'notes', 'rating', 'warnings',
                                                 'categories',
                                                 'fandoms', 'relationships', 'characters', 'additional_tags',
                                                 'language', 'published',
                                                 'word_count', 'chapter_count', 'comment_count', 'kudos_count',
                                                 'bookmarks_count',
                                                 'hits_count'])
                try:
                    old_db = pd.read_csv('output/story_db.csv')
                    story_db = pd.concat([old_db, story_db])
                except FileNotFoundError:
                    print('No existing story_db found, creating new one.')
                story_db.to_csv('output/story_db.csv', index=False)

            except ValueError:
                print(story_db)
                print('err')

        # Wait between pages to avoid overwhelming the server
        time.sleep(get_wait_time())

    # Final cleanup after scraping is complete
    output_cleanup.clean_files()

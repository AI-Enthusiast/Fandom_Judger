import time

import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import random
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_wait_time(quick=True):
    if quick:
        return random.uniform(19, 31)
    else:
        return random.uniform(30, 60)


def get_story_info(work_id, attempt=1):
    url_prefix = 'https://archiveofourown.org/works/'
    url_suffix = '?view_full_work=true'
    adult_suffix = '&view_adult=true'
    url = url_prefix + str(work_id) + url_suffix + adult_suffix
    raw = requests.get(url)
    bad_count = 0
    if raw.status_code != 200 or raw.url == 'https://archiveofourown.org/users/login?restricted=true':
        return None
    soup = bs(raw.text, 'html.parser')
    if soup.find('body').find('pre'):
        print('Retry later hit for work_id:', work_id)
        # wait for 5 minutes
        time.sleep(300)
        if attempt < 3:
            return get_story_info(work_id, attempt=attempt + 1)
        else:
            return None
    if soup.find('div', class_='system errors error-404 region'):
        return None

    warnings = soup.find('dd', class_='warning tags').text
    language = soup.find('dd', class_='language').text

    warnings = warnings.replace('\n\n', '')
    language = language.replace('\n', '')

    # if warnings is Underage or language is english, do not get the story
    if warnings == 'Underage' or language != 'English':
        story = ''
    else:
        try:
            story = soup.find('div', id='chapters').find('div', class_='userstuff').text
        except:
            story = ''
            # print('Error with work_id:', work_id)
            bad_count += 1
    try:
        title = soup.find('h2', class_='title heading').text
    except:
        title = None
        # print('Error with work_id:', work_id)
        bad_count += 1
    try:
        author = soup.find('h3', class_='byline heading').text
    except:
        author = None
        # print('Error with work_id:', work_id)
        bad_count += 1
    if bad_count > 2:  # if there are too many errors, we skip this work
        print('Error with work_id:', work_id)
        return None
    try:
        summary = soup.find('div', class_='summary module').text
    except:
        summary = None
    try:  # not all stories have notes
        notes = soup.find('div', class_='notes module').text
    except AttributeError:
        notes = None
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
    try:
        characters = soup.find('dd', class_='character tags')
        characters = characters.find_all('a')
        characters = [character.text for character in characters]
    except:
        characters = None
    try:
        additional_tags = soup.find('dd', class_='freeform tags')
        additional_tags = additional_tags.find_all('a')
        additional_tags = [tag.text for tag in additional_tags]
    except:
        additional_tags = None
    stats = soup.find('dl', class_='stats')
    published = stats.find('dd', class_='published').text
    word_count = stats.find('dd', class_='words').text
    chapter_count = stats.find('dd', class_='chapters').text
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
    story_info = get_story_info(work_id)
    if story_info:
        with open('output/stories/' + str(work_id) + '.txt', 'w') as f:
            f.write(story_info[1])
        return story_info[0]
    else:
        return [work_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None]


def multi_scrape_ids(work_ids):
    story_db = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_work_id, work_id): work_id for work_id in work_ids}
        # for future in tqdm(as_completed(futures), total=len(work_ids), desc="Scrapping works"):
        for future in as_completed(futures):
            story_db.append(future.result())
    return story_db


def get_work_ids(url, soup=None):
    if soup is None:
        raw = requests.get(url)
        soup = bs(raw.text, 'html.parser')
    works = soup.find_all('li', class_='work')
    work_ids = [work.find('a')['href'].split('/')[-1] for work in works]
    return work_ids


if __name__ == '__main__':
    # for i in range(10):
    #     rand_scrape(seed=42+i, n=5000)
    #     wait_time = 30*(i+1)
    #     print(f'Waiting for {wait_time} seconds')
    #     time.sleep(wait_time)

    all_works_url = 'https://archiveofourown.org/works/search?work_search%5Bquery%5D=&work_search%5Btitle%5D=&work_search%5Bcreators%5D=&work_search%5Brevised_at%5D=&work_search%5Bcomplete%5D=&work_search%5Bcrossover%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bword_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bfandom_names%5D=&work_search%5Brating_ids%5D=&work_search%5Bcharacter_names%5D=&work_search%5Brelationship_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Bcomments_count%5D=&work_search%5Bbookmarks_count%5D=&work_search%5Bsort_column%5D=_score&work_search%5Bsort_direction%5D=desc&commit=Search'
    page_prefix = 'https://archiveofourown.org/works/search?commit=Search&page='
    page_suffix = '&work_search%5Bbookmarks_count%5D=&work_search%5Bcharacter_names%5D=&work_search%5Bcomments_count%5D=&work_search%5Bcomplete%5D=&work_search%5Bcreators%5D=&work_search%5Bcrossover%5D=&work_search%5Bfandom_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bquery%5D=&work_search%5Brating_ids%5D=&work_search%5Brelationship_names%5D=&work_search%5Brevised_at%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bsort_column%5D=_score&work_search%5Bsort_direction%5D=desc&work_search%5Btitle%5D=&work_search%5Bword_count%5D='
    page = 2
    first = True
    # get the number of works
    raw = requests.get(all_works_url)
    soup = bs(raw.text, 'html.parser')
    num_works = soup.find('h3', class_='heading').text
    num_works = num_works.split(' ')[0]
    num_works_int = int(num_works.replace(',', ''))
    num_of_pages = num_works_int // 20 + 1
    for i in tqdm(range(num_of_pages), desc='Scrapping pages'):
        # for i in range(num_of_pages):
        attempts = 0
        work_ids = []
        while len(work_ids) == 0 and attempts < 3:
            if attempts > 0:
                time.sleep(300)
            if first:
                work_ids = get_work_ids(all_works_url, soup)
                first = False
            else:

                random_page = random.randint(1, num_of_pages)
                work_ids = get_work_ids(page_prefix + str(page) + page_suffix)
                page += 1
            attempts += 1
        if len(work_ids) == 0:
            print('No work_ids found')
            break

        story_db = multi_scrape_ids(work_ids)

        if len(story_db) != 0:
            try:
                old_db = pd.read_csv('output/story_db.csv')
                story_db = pd.DataFrame(story_db,
                                        columns=['work_id', 'title', 'author', 'summary', 'notes', 'rating', 'warnings',
                                                 'categories',
                                                 'fandoms', 'relationships', 'characters', 'additional_tags',
                                                 'language', 'published',
                                                 'word_count', 'chapter_count', 'comment_count', 'kudos_count',
                                                 'bookmarks_count',
                                                 'hits_count'])
                story_db = pd.concat([old_db, story_db])
                story_db.to_csv('output/story_db.csv', index=False)

            except ValueError:
                print(story_db)
                print('err')

        time.sleep(get_wait_time())

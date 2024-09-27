import pandas as pd
import glob
import os

# read in story_db.csv
story_db = pd.read_csv('output/story_db.csv')

# get the list of txt files in the output/stories directory
story_files = glob.glob('output/stories/*.txt')

# remove any files that are not in the story_db
for story_file in story_files:
    work_id = int(os.path.basename(story_file).replace('.txt', ''))
    if work_id not in story_db['work_id'].values:
        os.remove(story_file)
        print(f'Removed {story_file}')

# in the story_db drop all rows where everything but work_id is None
story_db = story_db.dropna(subset=['title', 'author', 'summary', 'notes', 'rating', 'warnings', 'categories', 'fandoms', 'relationships', 'characters', 'additional_tags', 'language', 'published', 'word_count', 'chapter_count', 'comment_count', 'kudos_count', 'bookmarks_count', 'hits_count'])

# order the story_db by hits_count then deduplicate by work_id
story_db = story_db.sort_values('hits_count', ascending=False).drop_duplicates('work_id')

# write the story_db back to a csv
story_db.to_csv('output/story_db.csv', index=False)
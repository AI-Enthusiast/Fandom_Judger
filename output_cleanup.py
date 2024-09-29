import pandas as pd
import glob
import os

def read_story_db(file_path):
    return pd.read_csv(file_path)

def remove_files_not_in_db(story_db, directory):
    story_files = glob.glob(os.path.join(directory, '*.txt'))
    for story_file in story_files:
        work_id = int(os.path.basename(story_file).replace('.txt', ''))
        if work_id not in story_db['work_id'].values:
            os.remove(story_file)
            print(f'Removed {story_file}')

def clean_story_db(story_db):
    story_db = story_db.dropna(subset=['title', 'author', 'summary', 'notes', 'rating', 'warnings', 'categories', 'fandoms', 'relationships', 'characters', 'additional_tags', 'language', 'published', 'word_count', 'chapter_count', 'comment_count', 'kudos_count', 'bookmarks_count', 'hits_count'])
    story_db = story_db.sort_values('hits_count', ascending=False).drop_duplicates('work_id')
    return story_db

def save_story_db(story_db, file_path):
    story_db.to_csv(file_path, index=False)

def clean_files(story_db_path = 'output/story_db.csv', story_dir = 'output/stories'):
    story_db = read_story_db(story_db_path)
    story_db = clean_story_db(story_db)
    remove_files_not_in_db(story_db, story_dir)
    save_story_db(story_db, story_db_path)


if __name__ == '__main__':
    clean_files()
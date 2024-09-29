import pandas as pd
import glob
import os


def read_story_db(file_path):
    return pd.read_csv(file_path)


def remove_files_not_in_db(story_db, directory):
    story_files = glob.glob(os.path.join(directory, '*.txt'))
    # remove any files that don't have a corresponding row
    bad_files = []
    for story_file in story_files:
        work_id = int(os.path.basename(story_file).replace('.txt', ''))
        if work_id not in story_db['work_id'].values:
            bad_files.append(story_file)
    remove_response = input(f'Remove {len(bad_files)} files from the directory? (y/n): ')
    if remove_response == 'y':
        for bad_file in bad_files:
            os.remove(bad_file)
    # likewise, remove any rows in the db that don't have a corresponding file
    bad_indexes = []
    for index, row in story_db.iterrows():
        work_id = row['work_id']
        story_file = os.path.join(directory, f'{work_id}.txt')
        if not os.path.exists(story_file):
            bad_indexes.append(index)

    remove_response = input(f'Remove {len(bad_indexes)} rows from the db? (y/n): ')
    if remove_response == 'y':
        story_db = story_db.drop(bad_indexes)
    story_db.to_csv('output/story_db.csv', index=False)


def clean_story_db(story_db):
    story_db = story_db.dropna(
        subset=['title', 'author', 'summary', 'notes', 'rating', 'warnings', 'categories', 'fandoms', 'relationships',
                'characters', 'additional_tags', 'language', 'published', 'word_count', 'chapter_count',
                'comment_count', 'kudos_count', 'bookmarks_count', 'hits_count'])
    story_db = story_db.sort_values('hits_count', ascending=False).drop_duplicates('work_id')
    return story_db


def save_story_db(story_db, file_path):
    story_db.to_csv(file_path, index=False)


def clean_files(story_db_path='output/story_db.csv', story_dir='output/stories'):
    story_db = read_story_db(story_db_path)
    story_db = clean_story_db(story_db)
    remove_files_not_in_db(story_db, story_dir)
    save_story_db(story_db, story_db_path)


if __name__ == '__main__':
    clean_files()

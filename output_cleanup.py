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
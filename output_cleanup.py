"""
Output cleanup script for the Fandom Judger project.
This script cleans up story database files and removes orphaned files/database entries.
"""
import pandas as pd
import glob
import os


def read_story_db(file_path):
    """
    Read the story database from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file containing story metadata
        
    Returns:
        pd.DataFrame: DataFrame containing story data
    """
    return pd.read_csv(file_path)


def remove_files_not_in_db(story_db, directory):
    """
    Remove orphaned files and database entries that don't have matching counterparts.
    
    This function performs bidirectional cleanup:
    1. Removes .txt files that don't have a corresponding database entry
    2. Removes database rows that don't have a corresponding .txt file
    
    Args:
        story_db (pd.DataFrame): DataFrame containing story metadata
        directory (str): Directory path containing story text files
    """
    # Find all .txt files in the directory
    story_files = glob.glob(os.path.join(directory, '*.txt'))
    
    # Find files that don't have a corresponding database row
    bad_files = []
    for story_file in story_files:
        # Extract work_id from filename (e.g., "12345.txt" -> 12345)
        work_id = int(os.path.basename(story_file).replace('.txt', ''))
        if work_id not in story_db['work_id'].values:
            bad_files.append(story_file)
    
    # Prompt user before removing orphaned files
    remove_response = input(f'Remove {len(bad_files)} files from the directory? (y/n): ')
    if remove_response == 'y':
        for bad_file in bad_files:
            os.remove(bad_file)
    
    # Find database rows that don't have a corresponding file
    bad_indexes = []
    for index, row in story_db.iterrows():
        work_id = row['work_id']
        story_file = os.path.join(directory, f'{work_id}.txt')
        if not os.path.exists(story_file):
            bad_indexes.append(index)

    # Prompt user before removing orphaned database rows
    remove_response = input(f'Remove {len(bad_indexes)} rows from the db? (y/n): ')
    if remove_response == 'y':
        story_db = story_db.drop(bad_indexes)
    
    # Save the cleaned database back to file
    story_db.to_csv('output/story_db.csv', index=False)


def clean_story_db(story_db):
    """
    Clean the story database by removing incomplete entries and duplicates.
    
    This function performs the following cleanup operations:
    1. Removes rows with missing data in any essential columns
    2. Sorts by hits_count in descending order (most popular first)
    3. Removes duplicate work_ids, keeping the highest hit count version
    
    Args:
        story_db (pd.DataFrame): Raw story database DataFrame
        
    Returns:
        pd.DataFrame: Cleaned story database DataFrame
    """
    # Remove rows with missing data in any of the essential columns
    story_db = story_db.dropna(
        subset=['title', 'author', 'summary', 'notes', 'rating', 'warnings', 'categories', 'fandoms', 'relationships',
                'characters', 'additional_tags', 'language', 'published', 'word_count', 'chapter_count',
                'comment_count', 'kudos_count', 'bookmarks_count', 'hits_count'])
    
    # Sort by popularity (hits_count) and remove duplicates, keeping the most popular version
    story_db = story_db.sort_values('hits_count', ascending=False).drop_duplicates('work_id')
    return story_db


def save_story_db(story_db, file_path):
    """
    Save the cleaned story database to a CSV file.
    
    Args:
        story_db (pd.DataFrame): Cleaned story database DataFrame
        file_path (str): Output path for the CSV file
    """
    story_db.to_csv(file_path, index=False)


def clean_files(story_db_path='output/story_db.csv', story_dir='output/stories'):
    """
    Main function to orchestrate the complete cleanup process.
    
    This function performs a comprehensive cleanup of both the story database
    and associated text files by:
    1. Loading the story database
    2. Cleaning the database (removing incomplete/duplicate entries)
    3. Removing orphaned files and database entries
    4. Saving the cleaned database back to file
    
    Args:
        story_db_path (str): Path to the story database CSV file
        story_dir (str): Directory containing story text files
    """
    # Load the story database
    story_db = read_story_db(story_db_path)
    
    # Clean the database by removing incomplete entries and duplicates
    story_db = clean_story_db(story_db)
    
    # Remove orphaned files and database entries
    remove_files_not_in_db(story_db, story_dir)
    
    # Save the final cleaned database
    save_story_db(story_db, story_db_path)


if __name__ == '__main__':
    # Execute the cleanup process when script is run directly
    clean_files()

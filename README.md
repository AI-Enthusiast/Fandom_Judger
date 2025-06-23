# Fandom Judger

A data collection and analysis project for scraping and analyzing fanfiction stories from Archive of Our Own (AO3).

## Overview

This project consists of tools to:
1. **Scrape fanfiction data** from AO3 including story metadata and full text
2. **Clean and organize** the collected data
3. **Analyze patterns** in fanfiction popularity and engagement metrics

## Project Structure

```
├── scraper.py           # Main web scraper for AO3 stories
├── output_cleanup.py    # Data cleaning and file management utilities
├── eda.rmd             # Exploratory data analysis in R
├── output/
│   ├── story_db.csv    # Generated CSV database of story metadata
│   └── stories/        # Directory containing individual story text files
└── README.md           # This file
```

## Features

### Web Scraping (`scraper.py`)
- Scrapes story metadata from AO3 including:
  - Title, author, summary, notes
  - Rating, warnings, categories, fandoms
  - Relationships, characters, additional tags  
  - Statistics (hits, kudos, comments, bookmarks, word count)
- Downloads full story text for each work
- Implements rate limiting and retry logic to respect AO3's servers
- Multithreaded processing for efficient data collection
- Filters out underage content and non-English stories

### Data Management (`output_cleanup.py`)
- Removes duplicate entries and maintains data integrity
- Syncs story files with database entries
- Cleans and validates collected data
- Interactive prompts for data cleanup decisions

### Analysis (`eda.rmd`)
- Exploratory data analysis using R
- Visualizations of story popularity metrics
- Statistical analysis of hits, kudos, and bookmarks relationships
- Identification of outliers and trends

## Installation

### Prerequisites
- Python 3.7+
- R (for analysis components)

### Python Dependencies
```bash
pip install requests beautifulsoup4 pandas tqdm
```

### R Dependencies
```r
install.packages(c("tidyverse", "ggplot2", "skimr"))
```

## Usage

### Basic Scraping
```bash
python scraper.py
```

This will:
1. Start scraping stories from AO3 sorted by hits (most popular first)
2. Save story metadata to `output/story_db.csv`
3. Save individual story texts to `output/stories/`
4. Automatically clean up data upon completion

### Data Cleanup
```bash
python output_cleanup.py
```

Manually run data cleaning to:
- Remove orphaned files and database entries
- Clean duplicate records
- Validate data integrity

### Analysis
Open `eda.rmd` in RStudio or run with knitr to:
- Explore story popularity distributions
- Analyze relationships between engagement metrics
- Generate visualizations

## Data Schema

The `story_db.csv` contains the following columns:
- `work_id`: Unique AO3 work identifier
- `title`: Story title
- `author`: Author name
- `summary`: Story summary
- `notes`: Author's notes
- `rating`: Content rating (General, Teen, Mature, Explicit)
- `warnings`: Content warnings
- `categories`: Relationship categories (Gen, F/M, M/M, etc.)
- `fandoms`: Source fandoms
- `relationships`: Character relationships
- `characters`: Character tags
- `additional_tags`: Freeform tags
- `language`: Story language
- `published`: Publication date
- `word_count`: Number of words
- `chapter_count`: Number of chapters
- `comment_count`: Number of comments
- `kudos_count`: Number of kudos (likes)
- `bookmarks_count`: Number of bookmarks
- `hits_count`: Number of hits (views)

## Rate Limiting & Ethics

This scraper implements responsible scraping practices:
- Random delays between requests (19-60 seconds)
- Respects AO3's rate limits and server capacity
- Includes retry logic with exponential backoff
- Filters content appropriately
- Does not scrape restricted or private works

**Please use responsibly** and in accordance with AO3's Terms of Service.

## Contributing

This project is for research and educational purposes. When contributing:
1. Maintain ethical scraping practices
2. Respect AO3's servers and community guidelines
3. Follow data privacy best practices
4. Test changes thoroughly before deploying

## License

This project is intended for academic and research purposes. Please respect AO3's Terms of Service and the intellectual property rights of fanfiction authors.

## Disclaimer

This tool is for research purposes only. Users are responsible for complying with Archive of Our Own's Terms of Service and respecting the rights of content creators. The authors of this project are not responsible for any misuse of this software.
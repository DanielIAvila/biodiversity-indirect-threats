# -*- coding: utf-8 -*-
"""
Created on Sat Mar  9 09:54:35 2024

@authors: Daniel Itzamna Avila Ortega (1,2) & Federico Grossi (3)
@institution: 
    - 1: Stockholm Resilience Centre | Stockholm University
    - 2: Global Economic Dynamics and the Biosphere | The Royal Swedish Academy of Sciences
    - 3: Helsinki Institute of Sustainability Science (HELSUS), University of Helsinki
@code: v3, Adapted for HPC/Slurm cluster execution
@Description:


@article: What is the evidence on indirect drivers of biodiversity loss worldwide? A systematic map protocol
@journal:
@DOI:
@corpus:
"""

import os
import gc
import pandas as pd
from tqdm import tqdm
from itertools import chain
from pyalex import Works, config

# Polite request data
config.email = "your.email@domain"

config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]
config.n_max_entities = None

# Raw OpenAlex URL
url = "https://api.openalex.org/works"

#%%
# Search string to use in OpenAlex
search = '(pressures OR threats OR footprint OR risk OR drivers OR stress OR impacts OR loss) AND (species OR ecosystems OR wildlife OR habitat) AND (hotspot OR map OR spatial OR direct OR indirect OR feedback OR long-distance OR human OR anthropogenic OR actions) AND (biodiversity OR conservation)'

# Columns to query from OpenAlex #https://docs.openalex.org/api-entities/works/work-object
query = ["id", "display_name", "title", "type_crossref", "type", "publication_date", 
         "publication_year", "doi", "language", "authorships", "corresponding_author_ids",
         "corresponding_institution_ids", "countries_distinct_count",
         "cited_by_count", "cited_by_api_url", "citation_normalized_percentile", 
         "relevance_score", "related_works", "referenced_works_count", "fwci",
         "referenced_works", "open_access", "apc_paid", "apc_list", "grants",
        "is_retracted", "institutions_distinct_count", "countries_distinct_count", 
        "primary_topic", "topics", "keywords",
        "primary_location"] 




def getCorpora(year, search, query):

    # Initialize an empty list to store our results as we get them
    all_results = []
    count_api_queries = 0
    
    # Initialize the API
    results = Works().filter(publication_year=year).search(search)

    # Cursor paging with PyAlex
    for record in chain(*results.paginate(per_page=200, n_max=None)):
        
        try:
            # Attempt to access 'abstract', continue if it fails
            abstract = record['abstract']
        except KeyError:
            # If there's a KeyError, skip this record
            continue
        
        # Extract the specified fields
        data = {key: record.get(key,  "N/A") for key in query}
        data['abstract'] = abstract
        
        all_results.append(data)
        count_api_queries += 1
        
    df = pd.DataFrame.from_dict(all_results)
        
    gc.collect()
        
    return df

# Define parent directory and folder to save files
dir_parent = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
config.project_datas_folder_path = os.path.join(dir_parent, "OpenAlex") # folder to save the dataset, must be empty

# Check if folder (OpenAlex) exists, if not create it
if not os.path.exists(config.project_datas_folder_path):
    os.makedirs(config.project_datas_folder_path)

# Check if the folder is empty, if yes continue and download files
if not os.listdir(config.project_datas_folder_path):
    print("Directory is empty, starting downloading dataset...")
else:
    raise ValueError("Directory not empty, can't donwload the dataset in " + config.project_datas_folder_path)
    
    
# Create list of years to download data
years = [year for year in range(1999,2025)]


for year in tqdm(years):
    
    # Define the filename for the current year
    filename = os.path.join(config.project_datas_folder_path, f'biorev_{year}.json')
    
    # Check if the file already exists
    if os.path.exists(filename):
        print(f"File for year {year} already exists, skipping download.")
        continue
    
    # If the file does not exist, print downloading message and process the data
    print(f"Downloading datasets year by year (from {years[0]} to {years[-1]}): downloading year {year}")
    
    # Get corpora for given year
    df_year = getCorpora(year, search, query)
    
    print(f"Total retrieved articles for year {year}:{len(df_year)}")
    
    # Save corpora to JSON
    df_year.to_json(config.project_datas_folder_path + f'/biorev_{year}.json', orient="columns")
    
    # Clean up memory
    gc.collect()

""" Finish code """

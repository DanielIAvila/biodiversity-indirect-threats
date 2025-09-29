# -*- coding: utf-8 -*-
"""
Created on Sat Mar  9 09:54:35 2024
Modified on Sat Sep 20 20:45:00 2025

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
import sys
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
    
    # Initialize the API
    results = Works().filter(publication_year=year).search(search)

    # Cursor paging with PyAlex
    # Using a tqdm progress bar for the API pagination
    # In a non-interactive log file, this will still print progress updates.
    pbar = tqdm(results.paginate(per_page=200, n_max=None), desc=f"Fetching records for {year}")
    for page in pbar:
        for record in page:
            try:
                # Attempt to access 'abstract', continue if it fails
                abstract = record['abstract']
            except (KeyError, TypeError):
                # If there's an error (e.g., abstract is None or missing), skip this record
                continue
            
            # Extract the specified fields
            data = {key: record.get(key, None) for key in query}
            data['abstract'] = abstract
            
            all_results.append(data)
            
    if not all_results:
        return pd.DataFrame() # Return empty DataFrame if no results found
        
    df = pd.DataFrame.from_dict(all_results)
        
    gc.collect()
        
    return df

def process_single_year(year_to_process):
    """
    Main logic to download and save data for a single year.
    """
    # Define parent directory and folder to save files
    home_dir = os.path.expanduser("~") 
    dir_parent = os.path.join(home_dir, "bioIndThr") 
    project_datas_folder_path = os.path.join(dir_parent, "OpenAlex_Parquet_Yearly")

    # Check if folder exists, if not create it
    if not os.path.exists(project_datas_folder_path):
        try:
            os.makedirs(project_datas_folder_path)
            print(f"Created directory: {project_datas_folder_path}")
        except FileExistsError:
            # This can happen in a race condition if two jobs start at the same time
            pass

    # Define the filename for the current year using the .parquet extension
    filename = os.path.join(project_datas_folder_path, f'biorev_{year_to_process}.parquet')
    
    # Check if the file already exists
    if os.path.exists(filename):
        print(f"File for year {year_to_process} already exists, skipping download.")
        return # Exit the function for this year
    
    # If the file does not exist, print downloading message and process the data
    print(f"Starting download for year: {year_to_process}")
    
    # Get corpora for given year
    df_year = getCorpora(year_to_process, search, query)
    
    # Check if the dataframe is empty
    if df_year.empty:
        print(f"No articles with abstracts found for year {year_to_process}. Skipping file creation.")
        return

    print(f"Total retrieved articles for year {year_to_process}: {len(df_year)}")
    
    # Save corpora to Parquet format.
    df_year.to_parquet(filename, engine='pyarrow', index=False)
    print(f"Successfully saved {filename}")
    
    # Clean up memory
    del df_year
    gc.collect()


if __name__ == "__main__":
    # --- Main execution block for cluster ---
    
    # The script expects the year to be passed as the first command-line argument
    if len(sys.argv) < 2:
        print("ERROR: Missing command-line argument for year.")
        print("Usage: python downloadData.py <year>")
        sys.exit(1) # Exit with an error code

    try:
        # Convert the command-line argument to an integer
        target_year = int(sys.argv[1])
    except ValueError:
        print(f"ERROR: Invalid year provided. '{sys.argv[1]}' is not a valid integer.")
        sys.exit(1)

    print(f"--- Starting job for year {target_year} ---")
    process_single_year(target_year)
    print(f"--- Finished job for year {target_year} ---")


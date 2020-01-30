# dataset-harvest-source.py
import csv
import json
import pprint
import requests

# Define CKAN URL
CKAN_URL = 'https://catalog.data.gov'

# CKAN API URL
CKAN_API_URL = CKAN_URL + '/api/3/action'

# Actions URLs
PACKAGE_SHOW = '/package_show?id='
PACKAGE_SEARCH = '/package_search?q=' 

# Search String
SEARCH_STR = 'dept of education'

# Organization Name List
org_list = ['ed-gov']

# Package Cache
dict_package_cache = {}

def package_search():
    url = CKAN_API_URL + PACKAGE_SEARCH + SEARCH_STR + '&rows=10000'

    # Make the HTTP request.
    response = requests.get(url)
    assert response.status_code == 200

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = response.json()

    # Check the contents of the response.
    assert response_dict['success'] is True
    result = response_dict['result']['results']

    return result

def extract_interesting_metadata(package_metadata):
    
    package_id = package_metadata['id']
    package_name = package_metadata['name']
    package_title = package_metadata['title']
    package_url = package_metadata['url']
    package_organization = package_metadata['organization']['title']
    package_groups = [group['title'] for group in package_metadata['groups']]
    package_harvest_source_id = str()

    if package_metadata['type'] == "dataset":
        for dict_extras in package_metadata['extras']:
            if dict_extras['key'] == 'harvest_source_id':
                package_harvest_source_id = str(dict_extras['value'])

    dict_package = {
        'id' : package_id,
        'name' : package_name,
        'title' : package_title,
        'url' : package_url,
        'organization' : package_organization,
        'groups' : package_groups,
        'harvest_source_id' : package_harvest_source_id
    }

    return dict_package

def read_package_metadata(package_id):
    
    # return from cache
    if package_id in dict_package_cache.keys():
        return dict_package_cache[package_id]
    
    url = CKAN_API_URL + PACKAGE_SHOW + str(package_id)

    # Make the HTTP request.
    response = requests.get(url)
    if response.status_code != 200:
        return None

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = response.json()

    # Check the contents of the response.
    assert response_dict['success'] is True
    result = response_dict['result']

    # caching
    dict_package_cache.update( { "package_id" : result } )

    return result

def create_csv_row(dataset_metadata, harvest_metadata):

    row = {
        'dataset_id' : dataset_metadata['id'],
        'dataset_name' : dataset_metadata['name'],
        'dataset_title' : dataset_metadata['title'],
        'datase_url' : dataset_metadata['url'], 
        'dataset_organization' : dataset_metadata['organization'],
        'dataset_groups' : dataset_metadata['groups'] ,
        'harvest_source_id' : dataset_metadata['harvest_source_id'], 
        'harvest_source_name' : harvest_metadata['name'],
        'harvest_source_title' : harvest_metadata['title'],
        'harvest_source_url' : harvest_metadata['url'],
        'harvest_source_organization' : harvest_metadata['organization'],
        'harvest_source_groups' : harvest_metadata['groups']
    }

    return row

def get_package_metadata(package_search_result):

    csv_rows = []

    for package in package_search_result:

        #if package['organization']['name'] not in org_list:
        #    continue

        dataset_metadata = extract_interesting_metadata(package_metadata=package)
        if(dataset_metadata['harvest_source_id'] == ""):
            print(f"Harvest source id is empty: {package['name']}")
            continue

        print(json.dumps(dataset_metadata, indent=4)) 

        harvest_metadata = read_package_metadata(package_id=dataset_metadata['harvest_source_id'])
        if harvest_metadata is None:
            continue
        
        harvest_metadata = extract_interesting_metadata(package_metadata=harvest_metadata)

        csv_row = create_csv_row(dataset_metadata=dataset_metadata, harvest_metadata=harvest_metadata)
        csv_rows.append(csv_row)

    return csv_rows

def write_csv(rows):
    # write results as CSV
    csvfile = open('datasets_harvest_source.csv', 'w')
    fieldnames = ['dataset_id','dataset_name','dataset_title','datase_url', 
                'dataset_organization', 'dataset_groups', 'harvest_source_id', 
                'harvest_source_name','harvest_source_title', 'harvest_source_url',
                'harvest_source_organization', 'harvest_source_groups'             
                ]

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    csvfile.close()

def run():

    package_search_result = package_search()
    print(f'{len(package_search_result)} datasets found in the search.')

    csv_rows = get_package_metadata(package_search_result)
    print(f'{len(csv_rows)} datasets found in the ed-gov organization.')

    write_csv(csv_rows)

if __name__ == '__main__':
    run()
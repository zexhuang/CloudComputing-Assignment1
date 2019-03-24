# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time

def processGrids(fpath):
        # read grids file
    # grids_features is a dictionary of tuples, storing grid_id as keys and coordinates-polygon pairs as tuple
    grids_features = {}
    with open(fpath, encoding = 'UTF-8') as json_file:
        grids_data = json.load(json_file)
        grids_features.update(map(lambda x: [x['properties']['id'], (x['geometry']['coordinates'][0], list(x['properties'].values())[1:])], grids_data['features']))
        # We still need to merge small polygon into a large one
    return grids_features

def processTwitters(fpath):
    # read twitter file
    with open(fpath, encoding = 'UTF-8') as json_file:
        twitter_data = json.load(json_file)
    return twitter_data

def main():
    beginninga_time = time.time()

    #grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    grids_file_path = r"D:\Download\CCC\melbGrid.json"
    #twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
    twitter_file_path = r'D:\Download\CCC\tinyTwitter.json'

    myGrids = processGrids(grids_file_path)
    myTwitter = processTwitters(twitter_file_path)

    end_time = time.time()

    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()
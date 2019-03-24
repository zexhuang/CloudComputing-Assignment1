# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time

def processGrids(fpath):
        # read grids file
    # grids_features is a dictionary of tuples, storing grid_id as keys and coordinates-polygon pairs as tuple
    grids_features = {}
    with open(fpath) as json_file:
        grids_data = json.load(json_file)
        for feature in grids_data['features']:
            coordinates = feature['geometry']['coordinates'][0]
            polygon = feature['properties'].values()  # We still need to merge small polygon into a large one
            grid_id = polygon.pop(2)
            grids_features.update({grid_id: (coordinates, polygon)})
    return grids_features

def processTwitters(fpath):
    # read twitter file
    with open(fpath) as json_file:
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


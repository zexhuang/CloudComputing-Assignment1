# Authors: Zexian Huang 
# Date: March 24 2019
import json
#from mpi4py import MPI
import time

def processGrids(fpath):
        # read grids file
    grids_features = {}
    with open(fpath) as json_file:
        grids_data = json.load(json_file)
        for feature in grids_data['features']:
            grids_features.update({feature['properties']['id']: feature['geometry']['coordinates'][0]})
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
    myGrids

    end_time = time.time()

    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()


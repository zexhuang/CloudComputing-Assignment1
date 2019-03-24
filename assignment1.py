# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time

def processGrids():
    # read grids file
    grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    with open(grids_file_path) as json_file:  
        grids_data = json.load(json_file)
        coordinates = grids_data['features']
    return coordinates

def processTwitters():
    # read twitter file
    twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/tinyTwitter.json'
    with open(twitter_file_path) as json_file:  
        twitter_data = json.load(json_file)

    # the original twitter data file has 1000 rows of data
    twitter_rows = twitter_data['rows']
    for twitter in twitter_rows:
        print(twitter)
            
    return twitter_data

def main():
    beginninga_time = time.time()

    myGrids = processGrids()
    myTwitter = processTwitters()

    end_time = time.time()
    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()
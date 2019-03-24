# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time
     
beginninga_time = time.time()
# read grids file
grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
with open(grids_file_path) as json_file:  
    grids_data = json.load(json_file)
# read twitter file
twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
with open(twitter_file_path) as json_file:  
    twitter_data = json.load(json_file)

grid = dict()
for feature in mg['features']:
    grid.update({feature['properties']['id']: feature['geometry']['coordinates'][0]})

end_time = time.time()

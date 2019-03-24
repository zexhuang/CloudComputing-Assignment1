# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time
     
beginninga_time = time.time()

grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
with open(grids_file_path) as json_file:  
    grids_data = json.load(json_file)

twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
with open(twitter_file_path) as json_file:  
    twitter_data = json.load(json_file)


end_time = time.time()
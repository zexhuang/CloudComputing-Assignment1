# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

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

# This function is for returning a list of large grids, such as grid A, B, C, D 
def largeGridsList(xmin, ymin, xmax, ymax):
    return list()

# This function is for returning a list of small areas, such as grid A1, A2, A3, A4
def smallGridsList(c1,c2,c3,c4,c5):
    return list()

def checkPointInPolygon (x,y):
    point = Point(x,y)
    print(point)

    # point = Point(0.5, 0.5)
    # polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    # print(polygon.contains(point)) -> boolean value

    return bool

def main():
    beginninga_time = time.time()

    # grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    grids_file_path = r"D:\Download\CCC\melbGrid.json"
    # twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
    twitter_file_path = r'D:\Download\CCC\tinyTwitter.json'

    myGrids = processGrids(grids_file_path)
    myTwitter = processTwitters(twitter_file_path)

    end_time = time.time()

    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()
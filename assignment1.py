# Authors: Zexian Huang 
# Date: March 24 2019
import json
from mpi4py import MPI
import time
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

def processGrids(fpath):
    # grids_features is a dictionary whose keys are ploygons and values are dictionary whose keys are grid_ids and values are coordinates
    grids_features = {}
    grids_coordinates = {}
    grids_ploygons = {}
    with open(fpath, encoding = 'UTF-8') as json_file:
        grids_data = json.load(json_file)
        grids_ploygons.update(map(lambda x: [x['properties']['id'], list(x['properties'].values())[1:]], grids_data['features']))
        grids_coordinates.update(map(lambda x: [x['properties']['id'], x['geometry']['coordinates'][0]], grids_data['features']))
        ploygons = pd.DataFrame(grids_ploygons)
        coordinates = pd.DataFrame(grids_coordinates)
        for name in ['A', 'B', 'C', 'D']:
            # 0'xmin', 1'ymin', 2'xmax', 3'ymax'
            ploygon = tuple(pd.concat([ploygons.filter(like = name).loc[[0, 2], :].min(axis = 1), ploygons.filter(like = name).loc[[1, 3], :].max(axis = 1)]))
            coordinate = coordinates.filter(like = name).to_dict()
            coordinate.update(map(lambda x: (x, list(coordinate[x].values())), coordinate.keys()))
            grids_features.update({ploygon: coordinate})
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
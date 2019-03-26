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
        json_file.close()
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
    twitter_features = {}
    with open(fpath, encoding = 'UTF-8') as json_file:
        twitter_data = json.load(json_file)
        for row in twitter_data['rows']:
            if row['doc']['entities']['hashtags']:
                twitter_features.update({tuple(row['value']['geometry']['coordinates']): row['doc']['entities']['hashtags'][0]['text']})
        # we still need to parse the json file in iteration
    return twitter_features

# This function is for returning a dic of large grids, such as grid A, B, C, D 
def largeGrids(grids_features:dict):
    name = ['A', 'B', 'C', 'D']
    largeGrids = []
    for coord in grids_features.keys():
        largeGrids.append(coord)
    
    return dict(zip(name,largeGrids))

def smallGrids(grids_features:dict):
    smallGrids = []
    for area in grids_features.values():
        smallGrids.append(area)

    return smallGrids

def checkPointInPolygon (x,y):
    point = Point(x,y)
    # point = Point(0.5, 0.5)
    # polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    # print(polygon.contains(point)) -> boolean value
    return bool

def main():
    beginninga_time = time.time()

    grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    # grids_file_path = r"D:\Download\CCC\melbGrid.json"
    twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
    # twitter_file_path = r'D:\Download\CCC\tinyTwitter.json'

    myGrids = processGrids(grids_file_path)
    myTwitter = processTwitters(twitter_file_path)

    largeGrids(myGrids)
    smallGrids(myGrids)

    end_time = time.time()

    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()
# Authors: Zexian Huang 
# Date: March 24 2019
import json
import time
import itertools
import pandas as pd
from mpi4py import MPI
from collections import Counter

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

import matplotlib.path as mplPath
import numpy as np

names = ["A","B","C","D"]

def processGrids(fpath):
    # read melbGrid file
    # grids_features is a dictionary whose keys are ploygons and values are dictionary whose keys are grid_ids and values are coordinates
    grids_features = {}
    grids_coordinates = {}
    grids_ploygons = {}
    with open(fpath, encoding = 'UTF-8') as json_file:
        grids_data = json.load(json_file)
        json_file.close()
        grids_ploygons.update(map(lambda x: [x['properties']['id'], list(x['properties'].values())[1:]], grids_data['features']))
        grids_coordinates.update(map(lambda x: [x['properties']['id'], list(map(lambda y: tuple(y), x['geometry']['coordinates'][0]))], grids_data['features']))
        ploygons = pd.DataFrame(grids_ploygons)
        coordinates = pd.DataFrame(grids_coordinates)
        for name in names:
            # 0'xmin', 1'ymin', 2'xmax', 3'ymax'
            ploygon = tuple(pd.concat([ploygons.filter(like = name).loc[[0, 2], :].min(axis = 1), ploygons.filter(like = name).loc[[1, 3], :].max(axis = 1)]))
            coordinate = coordinates.filter(like = name).to_dict()
            coordinate.update(map(lambda x: (x, list(coordinate[x].values())), coordinate.keys()))
            grids_features.update({ploygon: coordinate})

    return grids_features

def processTwitters(fpath):
    # read twitter file
    # twitter_features is a list of tuple whose element are tuple of coordinates and list of hashtags
    twitter_features = []

    if 'twitter-melb' in fpath:
        with open(fpath, encoding='UTF-8') as json_file:
            for idx, line in enumerate(itertools.islice(json_file, 1, None)):
                if line.startswith(']}'):
                    break
                row = json.loads(line.rstrip(',\n'))
                if row['doc']['coordinates']:
                    if row['doc']['entities']['hashtags']:
                        twitter_features.append((tuple(row['doc']['coordinates']['coordinates']),
                                                 [row['doc']['entities']['hashtags'][0]['text']]))
                    else:
                        twitter_features.append((tuple(row['doc']['coordinates']['coordinates']),
                                                 row['doc']['entities']['hashtags']))
            json_file.close()
    else:
        with open(fpath, encoding='UTF-8') as json_file:
            for line in itertools.islice(json_file, 1, None):
                if line.startswith(']}'):
                    break
                row = json.loads(line.rstrip(',\n'))
                if row['value']['geometry']['coordinates']:
                    if row['doc']['entities']['hashtags']:
                        twitter_features.append((tuple(row['value']['geometry']['coordinates']),
                                             [row['doc']['entities']['hashtags'][0]['text']]))
                    else:
                        twitter_features.append(
                            (tuple(row['value']['geometry']['coordinates']),
                             row['doc']['entities']['hashtags']))
            json_file.close()

    return twitter_features

def largeGrids(grids_features:dict):
    largeGrids = []
    for coord in grids_features.keys():
        largeGrids.append(coord)
    
    return dict(zip(names,largeGrids))

def smallGrids(grids_features:dict):
    smallGrids = {}
    for (name, area) in zip(names,grids_features.values()):
        smallGrids.update({name:area})

    return smallGrids

def checkPointsInPoly(poly,coord):
    polygon = np.array(poly)
    polyPath = mplPath.Path(polygon)
    point = coord
    acc = 0.001
    isIn = polyPath.contains_point(point,radius=acc) or polyPath.contains_point(point,radius=-acc)
    return isIn

def countPointsInGrids(largeGrids: dict, smallGrids: dict, twitters: list):
    # hashtagsDict is a dict of Counters of hashtags: {gird_id: Counter(hashtags1: freqs1, hashtags2: freqs2, ...)}
    hashtagsDict = {}
    # hashtagsDict is a dict of twitter number of each grid: {gird_id: num of twitters...}
    countDict = {}
    for s in smallGrids.values():
        for sid in s.keys():
            hashtagsDict[sid] = Counter()
            countDict[sid] = 0
    for twitter in twitters:
        pointX, pointY = twitter[0]
        # point = Point(twitter[0][0], twitter[0][1])
        point = [twitter[0][0],twitter[0][1]]
        hashtag = twitter[1]
        for name in names:
            if (largeGrids.get(name)[1] <= pointY <= largeGrids.get(name)[3]):
                sgrids = smallGrids[name]
                for sgrid, spolygon in sgrids.items():
                    # polygon = Polygon(spolygon)
                    # if polygon.contains(point):
                    polygon = [list(elem) for elem in spolygon]
                    if checkPointsInPoly(polygon, point):
                        countDict[sgrid] += 1
                        hashtagsDict[sgrid].update(hashtag)
                break
    return hashtagsDict, countDict

def main():
    beginninga_time = time.time()

    # grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    grids_file_path = r"D:\Download\CCC\melbGrid.json"
    # twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
    twitter_file_path = r'D:\Download\CCC\tinyTwitter.json'

    myGrids = processGrids(grids_file_path)

    mylargeGrids = largeGrids(myGrids)
    mySmallGrids = smallGrids(myGrids)

    myTwitter = processTwitters(twitter_file_path)

    twitterDict, twitterCount = countPointsInGrids(mylargeGrids, mySmallGrids, myTwitter)
    print("the hashTag count in grid C3 are %s" % twitterDict['C3'].most_common(5))
    print("the twitters count in grid C3 are %s" % twitterCount['C3'])

    end_time = time.time()
    used_time = end_time - beginninga_time
    print ("the processing time is %f seconds" % used_time)

if __name__== "__main__":
    main()
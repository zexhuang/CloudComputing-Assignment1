# Authors: Zexian Huang 
# Date: March 24 2019
import json
import time
import itertools
import pandas as pd
from mpi4py import MPI
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

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
        for name in ['A', 'B', 'C', 'D']:
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

    if 'big' in fpath:
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

# This function is for returning a dic of large grids, such as grid A, B, C, D 
def largeGrids(grids_features:dict):
    name = ['A', 'B', 'C', 'D']
    largeGrids = []
    for coord in grids_features.keys():
        largeGrids.append(coord)
    
    return dict(zip(name,largeGrids))

def smallGrids(grids_features:dict):
    smallGrids = {}
    for (name, area) in zip(["A","B","C","D"],grids_features.values()):
        smallGrids.update({name:area})

    return smallGrids

def checkPointInLargeGrids(largeGrids:dict, twitters:list):
    countA = countB = countC = countD = countNoArea = 0
    # areaA = areaB = areaC = areaD = [] I've tried this line should be OK 0w0
    areaA = []
    areaB = []
    areaC = []
    areaD = []

    for twitter in twitters:
        # coordinates
        pointX, pointY = twitter[0]
        hashTag = twitter[1]
        if (largeGrids.get("A")[1] <= pointY <= largeGrids.get("A")[3]):
            countA += 1
            areaA.append ((pointX,pointY,hashTag))
        elif (largeGrids.get("B")[1] <= pointY <= largeGrids.get("B")[3]):
            countB += 1
            areaB.append ((pointX,pointY,hashTag))
        elif (largeGrids.get("C")[1] <= pointY <= largeGrids.get("C")[3]):
            countC += 1
            areaC.append ((pointX,pointY,hashTag))
        elif (largeGrids.get("D")[1] <= pointY <= largeGrids.get("D")[3]):
            countD += 1
            areaD.append ((pointX,pointY,hashTag))
        else:
            countNoArea += 1
    
    print(countA)
    print(countB)
    print(countC)
    print(countD)
    print(countNoArea)

    return {"A":areaA, "B":areaB, "C":areaC, "D":areaD}

def main():
    beginninga_time = time.time()

    # grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    grids_file_path = r"D:\Download\CCC\melbGrid.json"
    # twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/smallTwitter.json'
    twitter_file_path = r'D:\Download\CCC\tinyTwitter.json'

    myGrids = processGrids(grids_file_path)
    myTwitter = processTwitters(twitter_file_path)

    mylargeGrids = largeGrids(myGrids)
    mySmallGrids = smallGrids(myGrids)

    twittersInLargeGrids = checkPointInLargeGrids(mylargeGrids, myTwitter)

    resultDict = {}
    for name in ["A","B","C","D"]:
        for twitter in twittersInLargeGrids[name]:
            point = Point(twitter[0],twitter[1])
            for key in mySmallGrids[name]:
                if key not in resultDict:
                    resultDict.update({key:list()})
                polygon = Polygon(mySmallGrids[name][key])
                if polygon.contains(point):
                    resultDict[key].append(twitter)
    # resultDict = {grid_id: Counter(term1: freqs, term2: freqs, term3: freqs...)}
    resultDict

    # C2:1414 C3:659 D3:116
    # total is 2426

    end_time = time.time()
    used_time = end_time - beginninga_time
    print (used_time)

if __name__== "__main__":
    main()
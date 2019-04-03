# Authors: Zexian Huang
# Date: March 24 2019
import json
import time
import itertools
import pandas as pd
from mpi4py import MPI
from collections import Counter

names = ["A", "B", "C", "D"]
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def processGrids(fpath):
    # read melbGrid file
    # grids_features is a dictionary whose keys are ploygons and values are dictionary whose keys are grid_ids and values are coordinates
    grids_features = {}
    grids_coordinates = {}
    char_ploygons = {}

    with open(fpath, encoding='UTF-8') as json_file:
        grids_data = json.load(json_file)
        json_file.close()
        char_ploygons.update(
            map(lambda x: [x['properties']['id'], list(x['properties'].values())[1:]], grids_data['features']))
        grids_coordinates.update(
            map(lambda x: [x['properties']['id'], list(map(lambda y: tuple(y), x['geometry']['coordinates'][0]))],
                grids_data['features']))
        char_ploygons = pd.DataFrame(char_ploygons)
        coordinates = pd.DataFrame(grids_coordinates)
        for name in names:
            char_ploygon = tuple(pd.concat([char_ploygons.filter(like=name).loc[[2], :].min(axis=1),
                                            char_ploygons.filter(like=name).loc[[3], :].max(
                                                axis=1)]))  # 0'ymin', 1'ymax'
            num_ploygons = {}
            for sg_name in coordinates.filter(like=name).columns.values:
                num_ploygon = list(map(lambda x: x[0], coordinates[sg_name].tolist()))
                num_ploygons.update({sg_name: (min(num_ploygon), max(num_ploygon))})  # 0'xmin',1'xmax'
            grids_features.update({char_ploygon: num_ploygons})

    return grids_features

def processTwitters(fpath):
    # read twitter file
    # twitter_features is a list of tuple whose element are tuple of coordinates and list of hashtags
    lines = []
    twitter_features = []

    if 'twitter-melb' in fpath:
        with open(fpath, encoding='UTF-8') as json_file:
            for idx, line in enumerate(itertools.islice(json_file, 1, None)):
                if line.startswith(']}'):
                    break
                lines.append(line)
                if len(lines) == size:
                    if rank == 0:
                        sca_list = lines
                    else:
                        sca_list = None
                    line = comm.scatter(sca_list, root=0)
                    row = json.loads(line.rstrip(',\n'))
                    if row['doc']['coordinates']:
                        if row['doc']['entities']['hashtags']:
                            twitter_features.append((tuple(row['doc']['coordinates']['coordinates']),
                                                     [row['doc']['entities']['hashtags'][0]['text']]))
                        else:
                            twitter_features.append((tuple(row['doc']['coordinates']['coordinates']),
                                                     row['doc']['entities']['hashtags']))
                    lines = []
            json_file.close()
    else:
        with open(fpath, encoding='UTF-8') as json_file:
            for line in itertools.islice(json_file, 1, None):
                if line.startswith(']}'):
                    break
                lines.append(line)
                if len(lines) == size:
                    if rank == 0:
                        sca_list = lines
                    else:
                        sca_list = None
                    line = comm.scatter(sca_list, root=0)
                    row = json.loads(line.rstrip(',\n'))
                    if row['value']['geometry']['coordinates']:
                        if row['doc']['entities']['hashtags']:
                            twitter_features.append((tuple(row['value']['geometry']['coordinates']),
                                                 [row['doc']['entities']['hashtags'][0]['text']]))
                        else:
                            twitter_features.append(
                                (tuple(row['value']['geometry']['coordinates']),
                                 row['doc']['entities']['hashtags']))
                    lines = []
            json_file.close()
    return twitter_features

def largeGrids(grids_features: dict):
    largeGrids = []
    for coord in grids_features.keys():
        largeGrids.append(coord)

    return dict(zip(names, largeGrids))

def smallGrids(grids_features: dict):
    smallGrids = {}
    for (name, area) in zip(names, grids_features.values()):
        smallGrids.update({name: area})

    return smallGrids

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
        hashtag = twitter[1]
        for name in names:
            if largeGrids.get(name)[0] <= pointY <= largeGrids.get(name)[1]:
                for sgrid, spolygon in smallGrids[name].items():
                    if spolygon[0] <= pointX <= spolygon[1]:
                        countDict[sgrid] += 1
                        hashtagsDict[sgrid].update(list(map(lambda x: x.lower(), hashtag)))
                        break
                break

    return hashtagsDict, countDict

def gatherFlatten(result: dict):
    gatherings = comm.gather(result, root=0)
    flatten = pd.DataFrame()
    if rank == 0:
        for idx, gathering in enumerate(gatherings):
            if idx == 0:
                flatten = pd.DataFrame.from_records([gathering])
            else:
                flatten = flatten.add(pd.DataFrame.from_records([gathering]), fill_value=0)
    return flatten

def main():
    beginninga_time = time.time()

    # grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
    grids_file_path = r"D:\Download\CCC\melbGrid.json"
    # twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/tinyTwitter.json'
    twitter_file_path = r'D:\Download\CCC\twitter-melb.json'

    myGrids = processGrids(grids_file_path)
    mylargeGrids = largeGrids(myGrids)
    mySmallGrids = smallGrids(myGrids)

    myTwitter = processTwitters(twitter_file_path)
    twitterDict, twitterCount = countPointsInGrids(mylargeGrids, mySmallGrids, myTwitter)
    hashtags_gather = gatherFlatten(twitterDict)
    count_gather = gatherFlatten(twitterCount)
    if rank == 0:
        print("Counting of tweets:\n", count_gather, "\nTop 5 hashtags in each grid:")
        for grid in hashtags_gather:
            print(grid, ":", hashtags_gather.iloc[0][grid].most_common(5))

        end_time = time.time()
        used_time = end_time - beginninga_time
        print("the processing time is %f seconds" % used_time)

if __name__ == "__main__":
    main()
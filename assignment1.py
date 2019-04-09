# Authors: Zexian Huang, Zhixin Zheng 
# Date: March 24 2019
# Purpose: This code is for COMP90024: Cluster and Cloud Computing Assignment1
import json
import time
import itertools
import pandas as pd
from mpi4py import MPI
from collections import Counter

# grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
# grids_file_path = r"D:\Download\CCC\melbGrid.json"
# twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/bigTwitter.json'
# twitter_file_path = r'D:\Download\CCC\bigTwitter.json'

grids_file_path = "melbGrid.json"
twitter_file_path = "bigTwitter.json"

def processGrids(fpath):
    # read melbGrid file
    # grids_features is a dictionary whose keys are ploygons and values are dictionary whose keys are grid_ids and values are coordinates
    grids_features = {}
    grids_coordinates = {}
    char_ploygons = {}

    with open(fpath, encoding='UTF-8') as json_file:
        grids_data = json.load(json_file)
        json_file.close()
        names = set(map(lambda x: x['properties']['id'][0],grids_data['features']))
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

    return grids_features, names

def processTwitters(fpath, communicator):
    # read twitter file
    # twitter_features is a list of tuple whose element are tuple of coordinates and list of hashtags
    twitter_features = []
    with open(fpath, encoding='UTF-8') as json_file:
        for idx, line in enumerate(itertools.islice(json_file, 1, None)):
            if line.startswith(']}'):
                break
            if (idx % communicator.size) == communicator.rank:
                whereIsCoor = line.find("coordinates\":{\"type\":\"Point\",\"coordinates\":[")
                whereIsText = line.find("\"text\":\"")
                if whereIsCoor != -1:
                    coordinates = (float(i) for i in line[whereIsCoor+44:whereIsCoor+70].split("]")[0].split(","))
                    text = line[whereIsText+8: whereIsText+148].split("\",")[0]
                    twitter_features.append((coordinates,
                                                # use set() to remove duplicated hashtags
                                                set(term for term in text.split(" ")[1:-1] if term.startswith('#'))))

    json_file.close()

    return twitter_features

def largeGrids(grids_features: dict, names: set):
    largeGrids = []
    for coord in grids_features.keys():
        largeGrids.append(coord)

    return dict(zip(names, largeGrids))

def smallGrids(grids_features: dict, names: set):
    smallGrids = {}
    for (name, area) in zip(names, grids_features.values()):
        smallGrids.update({name: area})

    return smallGrids

def countPointsInGrids(largeGrids: dict, smallGrids: dict, twitters: list, names: set):
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
                        # count number of twitters of each grids
                        countDict[sgrid] += 1
                        # count number of hashtags of each grids
                        hashtagsDict[sgrid].update(list(map(lambda x: x.lower()[1:], hashtag)))
                        break
                break

    return hashtagsDict, countDict

def gatherFlatten(result: dict, communicator):
    gatherings = communicator.gather(result, root=0)
    flatten = pd.DataFrame()
    if communicator.rank == 0:
        for idx, gathering in enumerate(gatherings):
            if idx == 0:
                flatten = pd.DataFrame.from_records([gathering])
            else:
                flatten = flatten.add(pd.DataFrame.from_records([gathering]), fill_value=0)
    return flatten

def mostCommon(hashtags: Counter(), k: int):
    # collect all items whose value is greater or equal to tops
    if hashtags:
        hashtags = hashtags.most_common()
        tops = hashtags[k-1][1]  # get the smallest value in top k values

    return list(itertools.takewhile(lambda x: x[1] >= tops, hashtags))

def main():
    beginninga_time = time.time()
    # process information of grids
    myGrids, gridNames = processGrids(grids_file_path)
    mylargeGrids = largeGrids(myGrids, gridNames)
    mySmallGrids = smallGrids(myGrids, gridNames)

    comm = MPI.COMM_WORLD

    myTwitter = processTwitters(twitter_file_path, comm)
    twitterDict, twitterCount = countPointsInGrids(mylargeGrids, mySmallGrids, myTwitter, gridNames)
    comm.Barrier()  # Stops every process until all processes have arrived
    hashtags_gather = gatherFlatten(twitterDict, comm)
    count_gather = gatherFlatten(twitterCount, comm)
    
    if comm.rank == 0:
        for grid, count in zip(count_gather, hashtags_gather):
            print(f'{grid} has {count_gather.iloc[0][grid]} postings, and its Top 5 hashtags are {mostCommon(hashtags_gather.iloc[0][grid], 5)}')

        end_time = time.time()
        used_time = end_time - beginninga_time
        print("the processing time is %f seconds" % used_time)

if __name__ == "__main__":
    main()
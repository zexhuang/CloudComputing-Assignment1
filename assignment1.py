# Authors: Zexian Huang
# Date: March 24 2019
import json
import time
import itertools
import pandas as pd
from mpi4py import MPI
from collections import Counter

# grids_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/melbGrid.json'
# grids_file_path = r"D:\Download\CCC\melbGrid.json"
# twitter_file_path = '/Users/Huangzexian/Downloads/CloudComputing/assignment1-remote/tinyTwitter.json'
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
        names = sorted(set(map(lambda x: x['properties']['id'][0], grids_data['features'])))
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

def processTwitters(fpath, communicator, largeGrids: dict, smallGrids: dict, names: set):
    # read twitter file
    # twitter_features is a list of tuple whose element are tuple of coordinates and list of hashtags
    twitterDict = {}
    for s in smallGrids.values():
        for sid in s.keys():
            twitterDict[sid] = {"counting": 0, "hashtags": Counter()}
    with open(fpath, encoding='UTF-8') as json_file:
        for idx, line in enumerate(itertools.islice(json_file, 1, None)):
            if line.startswith(']}'):
                break
            if (idx % communicator.size) == communicator.rank:
                whereIsCoor = line.find("coordinates\":{\"type\":\"Point\",\"coordinates\":[")
                whereIsText = line.find("\"text\":\"")
                if whereIsCoor != -1:
                    pointX, pointY = (float(i) for i in line[whereIsCoor+44:whereIsCoor+70].split("]")[0].split(","))
                    text = line[whereIsText+8: whereIsText+148].split("\",")[0]
                    hashtags = set(term for term in text.split(" ")[1:-1] if term.startswith('#'))
                    for name in names:
                        if largeGrids.get(name)[0] <= pointY <= largeGrids.get(name)[1]:
                            for sgrid, spolygon in smallGrids[name].items():
                                if spolygon[0] <= pointX <= spolygon[1]:
                                    # count number of twitters of each grids
                                    twitterDict[sgrid]["counting"] += 1
                                    # count number of hashtags of each grids
                                    twitterDict[sgrid]["hashtags"].update(
                                        list(map(lambda x: x.lower()[1:], hashtags)))
                                    break
                            break
                    else:
                        continue

    json_file.close()

    return twitterDict

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

def gatherFlatten(result: dict, communicator):
    gatherings = communicator.gather(result, root=0)
    flatten = {}
    if communicator.rank == 0:
        for idx, gathering in enumerate(gatherings):
            for grid in gathering.keys():
                if idx == 0:
                    flatten = gathering
                else:
                    flatten[grid]["counting"] += gathering[grid]["counting"]
                    flatten[grid]["hashtags"] += gathering[grid]["hashtags"]

    return flatten

def mostCommon(hashtags: Counter(), k: int):
    # collect all items whose value is greater or equal to tops
    if hashtags:
        # get the smallest value in top k values
        threshold = sorted(set(hashtags.values()))[-k] if k <= len(sorted(set(hashtags.values()))) else 0
        hashtags = hashtags.most_common()

    return list(itertools.takewhile(lambda x: x[1] >= threshold, hashtags))

def main():

    beginninga_time = time.time()
    # process information of grids
    myGrids, gridNames = processGrids(grids_file_path)
    mylargeGrids = largeGrids(myGrids, gridNames)
    mySmallGrids = smallGrids(myGrids, gridNames)

    comm = MPI.COMM_WORLD

    twitterDict = processTwitters(twitter_file_path, comm, mylargeGrids, mySmallGrids, gridNames)
    #comm.Barrier()  # Stops every process until all processes have arrived
    twitters_gather = gatherFlatten(twitterDict, comm)
    if comm.rank == 0:
        for grid in sorted(twitters_gather.items(), reverse=True, key=lambda d: d[1]["counting"]):
            print(f'{grid[0]} has {grid[1]["counting"]} postings, and its Top 5 hashtags are {mostCommon(grid[1]["hashtags"], 5)}')
        end_time = time.time()
        used_time = end_time - beginninga_time
        print("the processing time is %f seconds" % used_time)

if __name__ == "__main__":
    main()
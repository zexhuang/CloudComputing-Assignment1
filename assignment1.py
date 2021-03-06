# Authors: Zexian Huang, Zhixin Zheng
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
    """Process information of grids

    This function process melbGird.json,
    extracting names of large grids in alphabetical order ("names"),
    coordinates of each large grids ("largeGrids"),
    and coordinates of each small grids ("smallGrids").
    In this file, we denoted the grids whose names are like "A" as large grids,
    and the grids whose names are like "A1" as small grids
    """
    # "grids_features" is a dictionary
    # whose keys are names of ploygons and values are dictionary whose keys are grid_ids and values are coordinates
    grids_features = {}

    with open(fpath, encoding='UTF-8') as json_file:
        grids_data = json.load(json_file)
        json_file.close()

    # Extract names of large grids in alphabetical order, e.g. ('A', 'B', 'C', 'D')
    names = sorted(set(map(lambda x: x['properties']['id'][0], grids_data['features'])))

    # Extract names of large grids in alphabetical order, e.g. ('A', 'B', 'C', 'D')
    char_ploygons = {}  # "char_ploygons" stores coordinates of large grids
    char_ploygons.update(
        map(lambda x: [x['properties']['id'], list(x['properties'].values())[1:]], grids_data['features']))
    grids_coordinates = {}  # "grids_coordinates" stores coordinates of small grids
    grids_coordinates.update(
        map(lambda x: [x['properties']['id'], list(map(lambda y: tuple(y), x['geometry']['coordinates'][0]))],
            grids_data['features']))
    char_ploygons = pd.DataFrame(char_ploygons)
    coordinates = pd.DataFrame(grids_coordinates)
    # Extract maximum and minimum coordinates of Y and X from "char_ploygon" and "grids_coordinates" respectively
    for name in names:
        char_ploygon = tuple(pd.concat([char_ploygons.filter(like=name).loc[[2], :].min(axis=1),
                                        char_ploygons.filter(like=name).loc[[3], :].max(
                                            axis=1)]))  # 0'ymin', 1'ymax'
        num_ploygons = {}
        for sg_name in coordinates.filter(like=name).columns.values:
            num_ploygon = list(map(lambda x: x[0], coordinates[sg_name].tolist()))
            num_ploygons.update({sg_name: (min(num_ploygon), max(num_ploygon))})  # 0'xmin',1'xmax'
        grids_features.update({char_ploygon: num_ploygons})

    # Extract coordinates of large grids
    largeGrids = dict(zip(names, [coord for coord in grids_features.keys()]))

    # Extract coordinates of small grids
    smallGrids = {}
    smallGrids.update(map(lambda x, y: (x, y), names, grids_features.values()))

    return largeGrids, smallGrids, names

def processTwitters(fpath, communicator, largeGrids: dict, smallGrids: dict, names: set):
    """Process information of twitters,including extraction and counting

    This function process bigTwitter.json,
    extracting coordinates and hashtags of twitters
    and counting the posts in each small grids
    """
    # "grids_features" is a dictionary
    # whose keys are names of small grids and values are pairs of counting of grids and hashtags of grids
    twitterDict = {}
    for s in smallGrids.values():
        for sid in s.keys():
            twitterDict[sid] = {"counting": 0, "hashtags": Counter()}

    with open(fpath, encoding='UTF-8') as json_file:
        for idx, line in enumerate(itertools.islice(json_file, 1, None)):
            if line.startswith(']}'):  # Drop the useless last line
                break
            if (idx % communicator.size) == communicator.rank:
                # Find indices of coordinates and raw text
                whereIsCoor = line.find("coordinates\":{\"type\":\"Point\",\"coordinates\":[")
                whereIsText = line.find("\"text\":\"")
                if whereIsCoor != -1:
                    # Extract X and Y from coordinates, the numbers are no longer than 13 digits(including dot)
                    pointX, pointY = (float(i) for i in line[whereIsCoor+44:whereIsCoor+70].split("]")[0].split(","))
                    # Extract raw texts whose length are no longer than 140 characters
                    text = line[whereIsText+8: whereIsText+148].split("\",")[0]
                    # Extract hashtags in \SPACE#hashtagSPACE\ pattern from raw text
                    hashtags = set(term for term in text.split(" ")[1:-1] if term.startswith('#'))
                    # Count the number of twitters in each grids
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

def gatherFlatten(result: dict, communicator):
    """Flat the a list of results of each process
    """
    # Gather the results from each process
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
    """Return a list of most k common items including equal counts in hashtags Counter
    """
    if hashtags:
        # Get the smallest value in top k values
        threshold = sorted(set(hashtags.values()))[-k] if k <= len(sorted(set(hashtags.values()))) else 0
        hashtags = hashtags.most_common()  # Get a list of hashtags items in descending order

    return list(itertools.takewhile(lambda x: x[1] >= threshold, hashtags))

def main():

    beginninga_time = time.time()
    # Process information of grids
    mylargeGrids, mysmallGrids, gridNames = processGrids(grids_file_path)

    comm = MPI.COMM_WORLD
    # Process information of grids
    twitterDict = processTwitters(twitter_file_path, comm, mylargeGrids, mysmallGrids, gridNames)
    twitters_gather = gatherFlatten(twitterDict, comm)
    if comm.rank == 0:
        # Sort the grids statistics in descending order, and Print them
        for grid in sorted(twitters_gather.items(), reverse=True, key=lambda d: d[1]["counting"]):
            print(f'{grid[0]} has {grid[1]["counting"]} postings, and its Top 5 hashtags are {mostCommon(grid[1]["hashtags"], 5)}')
        end_time = time.time()
        used_time = end_time - beginninga_time
        print("the processing time is %f seconds" % used_time)

if __name__ == "__main__":
    main()
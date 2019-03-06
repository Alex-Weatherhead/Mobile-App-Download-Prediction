from maven_utilities import build
from map_reduce_utilities import split, discretize
from talking_data_map_reduce_utilities import aggregate, join

dataset = "raw_training"

if __name__ == "__main__":

    build()

    attributes = {
        "ip": "0",
        "app": "1",
        "device": "2",
        "os": "3",
        "channel": "4",
        "click_time": "5",
        "attributed_time": "6",
        "is_attributed": "7"
    }

    inputPathname = "data/{0}.csv".format(dataset)
    outputPathname = "data/{0}_split".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    splitIndex = attributes["click_time"]
    delimeter = " "
    limit = 2
    keepLast = True

    split(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, splitIndex, delimeter, limit, keepLast)

    attributes = {
        "ip": "0",
        "app": "1",
        "device": "2",
        "os": "3",
        "channel": "4",
        "click_date": "5",
        "click_time": "6",
        "attributed_time": "7",
        "is_attributed": "8"
    }

    inputPathname = "data/{0}_split".format(dataset)
    outputPathname = "data/{0}_split_discretized".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    discretizeIndex = attributes["click_time"]
    bins = "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25"

    discretize(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, discretizeIndex, bins)

    # Aggregate based on ip.

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/ip".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = attributes["ip"]
    targetIndex = 8
    postJoiningIndex = 1

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on app
    
    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/app".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = attributes["app"]
    targetIndex = 8
    postJoiningIndex = 2

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on channel

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/channel".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = attributes["channel"]
    targetIndex = 8
    postJoiningIndex = 3

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on click_date

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/clickdate".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = attributes["click_date"]
    targetIndex = 8
    postJoiningIndex = 4

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on click_time

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/clicktime".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = attributes["click_time"]
    targetIndex = 8
    postJoiningIndex = 5

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)
    
    # Aggregate based on device/os

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/device_os".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["device"], attributes["os"]])
    targetIndex = 8
    postJoiningIndex = 6

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on ip/channel

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/ip_channel".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["ip"], attributes["channel"]])
    targetIndex = 8
    postJoiningIndex = 7

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on ip/clicktime

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/ip_clicktime".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["ip"], attributes["click_time"]])
    targetIndex = 8
    postJoiningIndex = 8

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on app/channel
    
    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/app_channel".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["app"], attributes["channel"]])
    targetIndex = 8
    postJoiningIndex = 9

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on app/click_time

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/app_clicktime".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["app"], attributes["click_time"]])
    targetIndex = 8
    postJoiningIndex = 10

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

    # Aggregate based on app/device/os

    inputPathname = "data/{0}_split_discretized".format(dataset)
    outputPathname = "data/{0}_split_discretized_aggregate/app_device_os".format(dataset)
    overwriteOutputPath = True
    numberOfReducers = 1
    aggregationIndices = ",".join([attributes["app"], attributes["device"], attributes["os"]])
    targetIndex = 8
    postJoiningIndex = 11

    aggregate(inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex)

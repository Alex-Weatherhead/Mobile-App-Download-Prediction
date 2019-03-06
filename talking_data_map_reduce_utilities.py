from subprocess import call

def aggregate (inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, aggregationIndices, targetIndex, postJoiningIndex):
    
    if (overwriteOutputPath):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.talkingdata.AggregationTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-overwriteOutputPath", "-numberOfReducers", str(numberOfReducers),
            "-aggregationIndices", str(aggregationIndices), "-targetIndex", str(targetIndex), "-postJoiningIndex", str(postJoiningIndex)])

    else:

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.talkingdata.AggregationTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-numberOfReducers", str(numberOfReducers), "-aggregationIndices", str(aggregationIndices),
            "-targetIndex", str(targetIndex), "-joinIndex", str(joinIndex)])

def join (inputPathname, outputPathname, overwriteOutputPath, numberOfReducers):
    
    if (overwriteOutputPath):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.talkingdata.JoiningTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-overwriteOutputPath", "-numberOfReducers", str(numberOfReducers)])

    else:

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.talkingdata.JoiningTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-numberOfReducers", str(numberOfReducers)])
from subprocess import call

def split (inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, splitIndex, delimeter, limit, keepLast):

    if (overwriteOutputPath and keepLast):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.SplittingTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname,
            "-overwriteOutputPath", "-numberOfReducers", str(numberOfReducers),
            "-splitIndex", str(splitIndex), "-delimeter", delimeter, "-limit", str(limit), "-keepLast"])

    elif (overwriteOutputPath):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.SplittingTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname,
            "-overwriteOutputPath", "-numberOfReducers", str(numberOfReducers),
            "-splitIndex", str(splitIndex), "-delimeter", delimeter, "-limit", str(limit)])

    elif (keepLast):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.SplittingTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname,
            "-numberOfReducers", str(numberOfReducers), "-splitIndex", str(splitIndex),
             "-delimeter", delimeter, "-limit", str(limit), "-keepLast"])
    
    else:

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.SplittingTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname,
            "-numberOfReducers", str(numberOfReducers), "-splitIndex", str(splitIndex),
             "-delimeter", delimeter, "-limit", str(limit)])       

def discretize (inputPathname, outputPathname, overwriteOutputPath, numberOfReducers, discretizeIndex, bins):

    if (overwriteOutputPath):

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.DiscretizationTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-overwriteOutputPath", "-numberOfReducers", str(numberOfReducers),
            "-discretizeIndex", str(discretizeIndex), "-bins", bins])

    else:

        call(["hadoop", "jar", "target/practice-1.0.jar", "ds.mapreduce.DiscretizationTool",
            "-inputPathname", inputPathname, "-outputPathname", outputPathname, 
            "-numberOfReducers", str(numberOfReducers), "-discretizeIndex", str(discretizeIndex),
            "-bins", bins])


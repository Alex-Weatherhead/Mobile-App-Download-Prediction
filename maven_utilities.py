from subprocess import call

def build ():

    call(["mvn", "clean", "package"])
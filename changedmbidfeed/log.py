from time import ctime

def log(msg):
    print "%s : %s" % (ctime(), msg)

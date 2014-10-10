#!/usr/bin/env python

from readblocks import readblocks
import gzip

def _extractdate(f, tmpdict):
    ptagdata = ""
    for tag, data in readblocks(f, ['UI', 'PY']):
        if tag == "UI": ptagdata = data[0]
        if tag == "PY" and int(data[0]) >= 1990: tmpdict[ptagdata] = data[0]

def extractdate(lfn):
    res = {}
    for fn in lfn:
        f = open(fn, "r")
        _extractdate(f, res)
        f.close()
    return res

def _dumpcsv(fn, res):
    with open(fn, "w") as f:
        for k in res:
            f.write("%s,%s\n"%(k,res[k]))
            
if __name__ == "__main__":
    import sys
    sys.argv = sys.stdin.read().split()
    _dumpcsv("date.csv", extractdate(sys.argv))

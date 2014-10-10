from readblocks import readblocks
import gzip, hashlib

matchtags=['DE', 'ID', 'UT']
ident = hashlib.md5()

def _listterm(f, tmpdict):
    for tag, res in readblocks(f, matchtags):
        if tag == "UT": 
            put = res[0]
        else:
            for r in res:
                r = r.upper()
                if r in tmpdict:
                    tmpdict[r].append(put)
                else:
                    tmpdict[r] = [put]

def _dumpterms(resultdict, out):
    for term in resultdict:
        ident.update(term)
        # out : hashid term nbocc
        for artid in resultdict[term]:
            out.write('%s,%s,"%s"\n'%(ident.hexdigest(),artid,term.replace('\\', '\\\\').replace('"', '\\"')))

def listterm(lfn, outn=None, filtr=None):
    res = {}
    for fn in lfn:
        with gzip.open(fn, 'r') as f:
            _listterm(f, res)
    if filtr: res = dict([(t,v) for t, v in res.iteritems() if filtr(v)])    
    if outn:
        with open(outn, 'w') as f:
            _dumpterms(res, f)
    return res

if __name__ == "__main__":
    import sys
    sys.argv = sys.stdin.read().split()
    listterm(sys.argv, "terms.csv", lambda x: len(x)>1)

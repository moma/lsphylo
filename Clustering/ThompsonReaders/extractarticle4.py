from readblocks import readblocks
from extractdate import extractdate
import gzip
import chardet

def listarticle(f, dd):
    pres = {}
    for tag, data in readblocks(f, ['UT', 'TI', 'AB', 'ID', 'DE']):
        if tag == "UT":
            res = None
            if pres and pres["artid"] not in dd: 
                res = pres.copy()
                dd[pres["artid"]] = None
            pres = {}
            pres["artid"] = data[0]
            pres["issid"] = data[0][:10]
            pres["data"] = ""
            if res: yield res
        else:
            pres["data"] += " &%%%%& ".join(data).upper().replace("|", "-") #.replace('\\', '\\\\').replace('"', '\\"')
    yield pres

def dumparticle(d, out):
    out.write('%s|%s\n'%(d["artid"], d["data"]))

def article(lfn, datedict):
    dd = {}
    out = {}
    for y in range(1990, 2014):
        out[y] = open("article_%d.csv"%(y), "w")
    for fn in lfn:
        with open(fn, "r") as f:
            for art in listarticle(f, dd):
                try:
                    art["data"].decode('ascii')
                except UnicodeDecodeError:
                    try:
                        art["data"] = art["data"].decode(chardet.detect(art["data"])["encoding"]).encode('utf-8')
                    except UnicodeError:
                        pass
                except:
                    pass
                try:
                    outy = out[int(datedict[art["issid"]])]
                except:
                    continue
                dumparticle(art, outy)


if __name__ == "__main__":
    import sys
    sys.argv = sys.stdin.read().split()
    article(sys.argv, extractdate(sys.argv))

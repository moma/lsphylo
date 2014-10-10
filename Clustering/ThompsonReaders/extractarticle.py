from readblocks import readblocks
import gzip

def listarticle(f, dd):
    pres = {}
    for tag, data in readblocks(f, ['UT', 'TI', 'AB']):
        if tag == "UT":
            res = None
            if pres and pres["artid"] not in dd: 
                res = pres.copy()
                dd[pres["artid"]] = None
            pres = {}
            pres["artid"] = data[0]
            pres["issid"] = data[0][:10]
            if res: yield res
        elif tag == "TI":
            pres["ti"] = data[0].upper().replace('\\', '\\\\').replace('"', '\\"')
        elif tag == "AB":
            pres["ab"] = data[0].upper().replace('\\', '\\\\').replace('"', '\\"')
    yield pres

def dumparticle(d, out):
    out.write('%s,%s,"%s","%s"\n'%(d["artid"], d["issid"], d["ti"], d["ab"]))

def article(lfn, outn):
    dd = {}
    with open("article.csv", "w") as out:
        for fn in lfn:
            with gzip.open(fn, "r") as f:
                for art in listarticle(f, dd):
                    try:
                        dumparticle(art, out)
                    except:
                        pass

if __name__ == "__main__":
    import sys
    sys.argv = sys.stdin.read().split()
    article(sys.argv, "article.csv")

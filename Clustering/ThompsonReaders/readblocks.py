# -*- coding: utf-8 -*-

# Fonction readblocks
# readblocks(f, filtre) : generateur (ID, data)

# f: un objet File
# filtre: None (par défaut) ou une liste d'ID
#   Si filtre n'est pas mentionné, renvoit tous les blocs
#   Sinon renvoit seulement les blocs dont l'ID est dans filtre
# ID: chaine de deux caractères différent de "--", apparait 
#     au début de chaque ligne du fichier.
#  NB: Le troisième caractère de chaque ligne est ignoré
# data: Une liste de données
#
# Exemple: f contient:
# AA donnee1
# BB donneel
# -- ongue
# BB donnee3
# readblocks(f, ['BB']) renvoit un générateur générant:
#   ('BB', ['donneelongue', 'donnee3'])

def readblocks(f, filtr=None):
    buf = f.readline()
    pres = []
    ptag = ""
    pdata =""
    skip = False
    while buf:
        tag = buf[:2]
        if tag == "--" and not skip:
            pdata += buf[3:-1]
        elif tag == ptag and not skip:
            pres.append(pdata)
            pdata = buf[3:-1]
        elif not tag == ptag and not tag == "--":
            res = None
            if not skip:
                if pres or pdata:
                    pres.append(pdata) 
                    res = ptag, pres
            ptag = tag
            pdata = buf[3:-1]
            pres = []
            skip = filtr and not tag in filtr
            if res: yield res
        buf = f.readline()
    if not skip: 
        pres.append(pdata)
        yield ptag, pres

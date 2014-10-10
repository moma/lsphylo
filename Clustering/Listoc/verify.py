#!/usr/bin/env python

import csv

def indexfile(fn):

	res = {}
	with open(fn, 'rb') as csvfile:
		r = csv.reader(csvfile, delimiter='|', quoting=csv.QUOTE_NONE)
		for row in r:
			if len(row) is not 2:	continue
			if row[0] in res:	raise ValueError(row[0])
			res[row[0]] = row[1]
	return res

def readfile(fn):

	res = []
	with open(fn, 'rb') as csvfile:
		r = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in r:
#			print row
#			return
			if len(row) is 2: res.append((row[0], row[1]))
	return res

if __name__ == "__main__":
	import sys
	import random
	random.seed()
	print sys.argv
	articles = indexfile(sys.argv[1])
	terms = indexfile(sys.argv[2])
	result = readfile(sys.argv[3])
	print len(articles)
	print len(terms)
	print len(result)
#	result = random.sample(result, 10000)

	for akey, tkey in result:
		if akey in articles and tkey in terms and not terms[tkey] in articles[akey]:
			raise ValueError("%s, %s: '%s' is not in '%s'"%(akey, tkey, terms[tkey], articles[akey]))
	sys.exit(0)

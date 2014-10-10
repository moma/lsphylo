import chardet
import sys

data = open('article_noascii.csv', 'r')
line = data.readline()
while line:
   try:
      print line.decode(chardet.detect(line)["encoding"]).encode('utf-8')[:-1]
      line = data.readline()
   except:
      sys.stderr.write("Article %s: encoding doesn't match\n"%line[:15])

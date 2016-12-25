#!/usr/bin/env python
import sys

f = open("english.stop")
content = f.read()
stopwords = content.split("\n")
for line in sys.stdin:
	theline = line.strip()
	line = [theline]
	rmsinput = [" ".join([w for w in t.split() if not w in stopwords]) for t in line]
	print rmsinput[0]

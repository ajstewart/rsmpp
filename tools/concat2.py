#!/usr/bin/env python

import sys
import pyrap.tables as pt

if __name__ == "__main__":
	print "Concatenate MeasurementSets\n"
	# try:
	t = pt.table(sys.argv[2])
	t.sort('TIME').copy(sys.argv[1], deep = True)
	t.close
	out=pt.table(sys.argv[1], readonly=False)
	for i in sys.argv[3:]:
		temp=pt.table(i)
		temp.sort('TIME').copyrows(out)
		temp.close()
	out.close()
	# except:
	# 	print "Usage: concat.py <output.MS> <input.MS> [input.MS] ..."

#!/usr/bin/env python

import numpy as np
import lofar.parmdb as pdb
import optparse, os
import pylab as pl

usage = "usage: %prog [options] instrumentdb skymodel"
parser = optparse.OptionParser(usage)
parser.add_option("-x", "--maxgain", help="Gain amp. max. Stations above this will be flagged. [20]", action="store", type="float", default=20.)
parser.add_option("-f", "--doflag", help="Actually do the flagging? [False]", action="store_true", default=False)
parser.add_option("-o", "--outfile", help="The name of the output skymodel file. [<input_skymodel>.adjusted]", action="store", type="string", default=None)
(opts, args) = parser.parse_args()

if len(args) == 0:
	raise Exception("Must pass an instrument table as an argument")

badcutoff = opts.maxgain
doflag = opts.doflag

ststr = '[CR]S'

fn = args[0]
mdfn = args[1]

if opts.outfile is None:
	outfn = mdfn+'.adjusted'
else:
	outfn = opts.outfile

itab = pdb.parmdb(fn)

ddgs0real = itab.getValues('DirectionalGain:0:0:Real:'+ststr+'*')

temp = ddgs0real.keys()[0].split(':')[5] # Returns one direction name

temp = itab.getValues('DirectionalGain:0:0:Real:'+ststr+'*:'+temp).keys()
stations = list()
for i in range(len(temp)):
	stations.append(temp[i].split(':')[4]) # returns the station name

stations = list(set(stations)) # to remove dupes, just in case

temp = itab.getValues('DirectionalGain:0:0:Real:'+stations[0]+':*').keys()
dirs = list()
for i in range(len(temp)):
	dirs.append(temp[i].split(':')[5])

dirs = list(set(dirs))

vals = dict()
#print stations

badstations = list()

for i in range(len(dirs)):
	val0 = 0
	val1 = 0
	ctr = 0
	for j in range(len(stations)):
		# must be a better way to do this... but this returns gain values for a single pair of station and direction
		ddgs0real = itab.getValues('DirectionalGain:0:0:Real:'+stations[j]+':'+dirs[i]).items()[0][1]['values'].flatten()
		ddgs1real = itab.getValues('DirectionalGain:1:1:Real:'+stations[j]+':'+dirs[i]).items()[0][1]['values'].flatten()
		ddgs0imag = itab.getValues('DirectionalGain:0:0:Imag:'+stations[j]+':'+dirs[i]).items()[0][1]['values'].flatten()
		ddgs1imag = itab.getValues('DirectionalGain:1:1:Imag:'+stations[j]+':'+dirs[i]).items()[0][1]['values'].flatten()
		g00 = np.zeros(len(ddgs0real), dtype=complex)
		g11 = np.zeros(len(ddgs1real), dtype=complex)
		g00.real = ddgs0real
		g00.imag = ddgs0imag
		g11.real = ddgs1real
		g11.imag = ddgs1imag
		if max(abs(g00)) > badcutoff or max(abs(g11)) > badcutoff:
			badstations.append('!'+stations[j])
		else:
			ctr+=1
			val0 += np.mean(np.abs(g00))
			val1 += np.mean(np.abs(g11))
		#pl.plot(abs(g00))
		#pl.plot(abs(g11))
	#pl.show()
	vals.update({dirs[i]: 0.5*(val0 + val1)/ctr})
	print "Dir: "+dirs[i]+" mean amp. gain = "+str(val0/ctr) + ", " + str(val1/ctr)

badstations = list(set(badstations))
# read in skymodel
# loop through and find sources that have been peeled
# rescale the flux
# write a new file with the new lines

outf = open(outfn, 'w')

for line in open(mdfn, 'r'):
	if line[0] == '#':
		outf.write(line)
		outf.write('\n')
	elif len(line) <= 1: continue
	else:
		sline = line.split(',')
		if dirs.count(sline[0]) > 0:
			sline[4] = str(float(sline[4])*vals[sline[0]])
			newline = ', '.join(sline)
			outf.write(newline)
outf.close()

basefn = fn.strip('/')[:-11]
#print badstations
if len(badstations)>0 and doflag:
	print "Flagging stations: ", badstations
	bs = '; '.join(badstations)
	os.system('mv '+basefn+' '+basefn+'.copy')
	os.system('msselect in='+basefn+'.copy out='+basefn+' baseline=\''+bs+'\' deep=True | tee msselect_float_solutions.log')



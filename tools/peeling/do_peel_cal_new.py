#! /usr/bin/env python

import subprocess
import os
import sys
import multiprocessing

#cutoff = sys.argv[1]

peeling = '/home/mrbell/bin/peeling_new.py'
concat = '/home/mrbell/bin/concat.py'
float_sols = '/home/mrbell/bin/float_solutions.py'

# Get rid of previously concat-ed files
os.system('rm -rf ./CONCAT_PREPEEL.MS')
os.system('rm -rf ./CONCAT_PEELED.MS')

# List MS files
fns = os.listdir('./msfiles/')
if len(fns) == 0:
    raise IOError('No valid files found in this directory!')

dosplit = True
nfns = len(fns)
fns_copy = list(fns)
for indx in range(nfns): # remove any non MS files
    fn = fns_copy[indx]
    if fn[len(fn)-5:len(fn)].lower() == 'dical':
        dosplit = False # if split has already been done, don't bother doing it again...
    if fn[len(fn)-2:len(fn)].lower() != 'ms':
        fns.remove(fn)

nfns = len(fns)

# concat prepeeled files
infiles = list()
for i in range(nfns):
    infiles.append('./msfiles/'+fns[i])
cmdlist = [concat, './CONCAT_PREPEEL.MS'] + infiles
subprocess.call(cmdlist)

if dosplit:
    # split the CORRECTED_DATA column off into the DATA column of a new MS file
    for i in range(nfns):
        f = open('./split.NDPPP', 'rw')
        newlines = []
        for line in f:
            if 'msin = ' in line:
                line = 'msin = ./msfiles/' + fns[i] + '\n'
            if 'msout = ' in line:
                line = 'msout = ./msfiles/' + fns[i] + '.DICAL\n'
            newlines.append(line)
                    
        f.close()
        f = file('./split.NDPPP', 'w')
        f.writelines(newlines)
        f.close()
        subprocess.call(['NDPPP', './split.NDPPP'])

def kickoff(x):
	sys.stdout = open(x + ".calibrate-stand-alone.log", "w")
	subprocess.call(['calibrate-stand-alone','-f','./msfiles/'+ x +'.DICAL','./peeling.parset','./peeling.skymodel'])

def kickoff_step2(x):
	sys.stdout = open(x + ".float_solutions.log", 'w')
	subprocess.call([float_sols, '-f', '-o', x+'.skymodel', './msfiles/'+x+'.DICAL/instrument/', 'peeling.skymodel'])

def kickoff_step3(x):
	sys.stdout = open(x + ".calibrate-stand-alone_step2.log", "w")
	subprocess.call(['calibrate-stand-alone','-f','./msfiles/'+ x +'.DICAL','./peeling_step2.parset',x+'.skymodel'])

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count)
pool.map(kickoff, fns)
pool.map(kickoff_step2, fns)
pool.map(kickoff_step3, fns)

# concat postpeeled files
infiles = list()
for i in range(nfns):
    infiles.append('./msfiles/'+fns[i] + '.DICAL')
cmdlist = [concat, './CONCAT_PEELED.MS'] + infiles
subprocess.call(cmdlist)

#!/usr/bin/env python

import optparse, subprocess, time, os, sys, glob
from multiprocessing import Pool
import pyrap.tables as pt
from pyrap.quanta import quantity
from datetime import datetime

usage = "usage: python %prog [options] html.txt"
description="Script to quickly fetch data from the LTA using wget or grid tools. Currently does not handle corrupted files but will obtain missing files."
vers="1.1.0"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("--check-only", action="store_true", dest="checkonly", default=False, help="Perform a check for missing data only [default: %default].")
parser.add_option("-c", "--check-attempts", action="store", type="int", dest="attempts", default=10, help="How many attempts to fetch missing files [default: %default].")
parser.add_option("-d", "--delay", action="store", type="int", dest="delay", default=120, help="Time between each fetch attempt in seconds [default: %default].")
parser.add_option("-m", "--method", action="store", type="choice", dest="ltameth", choices=["html", "srm"], default="html", help="Select whether to use wget ('html') or Grid tools ('srm') [default: %default].")
parser.add_option("-n", "--njobs", action="store", type="int", dest="ncpus", default=5, help="How many to attempt to fetch at once [default: %default].")
parser.add_option("-p", "--prepare", action="store_true", dest="prepare", default=True, help="Prepare and organise data for rsmpp [default: %default].")
(options, args) = parser.parse_args()

def countdown(wait):
	"""Funky countdown timer"""
	for remaining in range(wait, -1, -1):
	    sys.stdout.write("\r")
	    sys.stdout.write("Will attempt to fetch files in {:3d} seconds...".format(remaining)) 
	    sys.stdout.flush()
	    time.sleep(1)

def fetch(file):
	"""Simple wget get line"""
	print "Fetching {0}...".format(file.split("/")[-1])
	subprocess.call("wget {0} > /dev/null 2>&1".format(file), shell=True)
	
def gridfetch(file):
	"""Simple wget get line"""
	print "Fetching {0}...".format(file.split("/")[-1])
	subprocess.call("srmcp {0} > /dev/null 2>&1".format(file), shell=True)
	
def untar(file):
	"""Simple wget get line"""
	subprocess.call("tar --force-local -xvf {0} > /dev/null 2>&1".format(file), shell=True)

def rename1(SB):
	SBtable=pt.table("{0}/OBSERVATION".format(SB), ack=False)
	newname=SBtable.col("LOFAR_FILENAME")[0]
	SBtable.close()
	if newname.endswith(".MS"):
		newname+=".dppp"
	subprocess.call(["mv", SB, newname])
	
def organise(SB):
	obsid=SB.split("_")[0]
	subprocess.call(["mv", SB, os.path.join(obsid, SB)])
	
def deletefile(file):
	"""Only files not directories"""
	os.remove(file)
	
def fetchantenna(period):
	print "Fetching fixinfo file..."
	if period==1:
		subprocess.call("wget http://www.astron.nl/sites/astron.nl/files/cms/fixinfo.tar > /dev/null 2>&1", shell=True)
	elif period==2:
		subprocess.call("wget http://www.astron.nl/sites/astron.nl/files/cms/fixbeaminfo_March2015.tar > /dev/null 2>&1", shell=True)
	subprocess.call("tar xvf fixinfo.tar", shell=True)
	
def fixantenna(ms):
	print "Correcting Antenna Table for {0}...".format(ms.split("/")[-1])
	subprocess.call("./fixbeaminfo {0}".format(ms), shell=True)
	
workers=Pool(processes=options.ncpus)

#read in all the html files the user wishes
files=args[:]
initfetch=[]
for file in files:
	f=open(file, 'r')
	initfetch+=[i.rstrip('\n') for i in f]
	f.close()

#Time range of data which needs the antenna table corrected
antenna_range=[4867430400.0, 4898793599.0]
antenna_range2=[4928947200.0, 4931625599.0]

#perform initial fetch of all data if not checkonly
if not options.checkonly:
	workers.map(fetch, initfetch)
else:
	print "Performing check for missing files only"

#loop to check for missing files from the list
for j in range(options.attempts):
	print "----------------------------------------------------------------------------------------"
	print "Running Missing File Check {0} of {1}".format(j+1, options.attempts)
	if options.ltameth=="html":
		tofetch=[k for k in initfetch if not os.path.isfile('SRMFifoGet'+k.split('SRMFifoGet')[-1].replace('/', '%2F'))]
	else:
		tofetch=[k for k in initfetch if not os.path.isfile(k.split('file:///')[-1])]
	if len(tofetch) < 1:
		print "0 files remain to fetch"
		print "All files obtained!"
		break
	else:
		print "{0} files remain to fetch:".format(len(tofetch))
		print "----------------------------------------"
		for g in tofetch:
			print g.split("/")[-1]
		print "----------------------------------------"
		countdown(options.delay)
		print "\n"
		workers.map(fetch, tofetch)

print "LTA fetch complete!"

if options.prepare:
	#Need to prepare data for pipeline: untar -> rename -> organise into dirs
	print "Preparing data for rsmpp use..."
	ltaoutput=sorted(glob.glob("*.tar"))
	print "Unpacking data..."
	for tar in ltaoutput:
		#Untar files one at a time as doing multiple really hits disc writing speed
		untar(tar)
	ltaoutput2=sorted(glob.glob("*.MS"))
	if len(ltaoutput2)<1:
		#Stop the process if something has gone wrong and no .MS files are present
		print "No data files detected after unpacking! Did the download work?"
		sys.exit()
	else:
		print "Renaming LTA output..."
		lta_workers=Pool(processes=options.ncpus)
		lta_workers.map(rename1, ltaoutput2)
		print "Organising files..."
		ltaoutput3=sorted(glob.glob("*.dppp"))
		#Obtain a list of unique IDs
		ltaobsids=[ltams.split("_")[0] for ltams in ltaoutput3]
		uniq_ltaobsids=sorted(list(set(ltaobsids)))
		antenna_corrections=[]
		for lta_id in uniq_ltaobsids:
			#Check if directory already exists
			if os.path.isdir(lta_id):
				print "Obs ID directory {0} already exists in data directory - will not overwrite or move files.".format(lta_id)
				print "Please check and organise the downloaded data - rsmpp-rename.py can help with this."
				print "Once done the pipeline can be re-ran with LTA mode off, just point to the data directory."
				sys.exit()
			else:	
				os.mkdir(lta_id)
				ms_example=sorted(glob.glob("{0}*.dppp".format(lta_id)))[0]
				temp=pt.table(ms_example+'/OBSERVATION', ack=False)
				tempst=float(temp.getcell("LOFAR_OBSERVATION_START", 0))
				temp.close()
				if tempst >= antenna_range[0] and tempst <= antenna_range[1]:
					antenna_corrections.append(lta_id)
					print "{0}\t{1}\tAntenna Tables Correction Required".format(ms_example, datetime.utcfromtimestamp(quantity('{0}s'.format(tempst)).to_unix_time()))
					periodtocorr=1
				elif tempst >= antenna_range2[0] and tempst <= antenna_range2[1]:
					antenna_corrections.append(lta_id)
					print "{0}\t{1}\tAntenna Tables Correction Required".format(ms_example, datetime.utcfromtimestamp(quantity('{0}s'.format(tempst)).to_unix_time()))
					periodtocorr=2
				else:
					print "{0}\t{1}\tAntenna Tables Correction Not Required".format(ms_example, datetime.utcfromtimestamp(quantity('{0}s'.format(tempst)).to_unix_time()))
		lta_workers.map(organise, ltaoutput3)
		print "Organised!"
		if len(antenna_corrections) > 0:
			print "Performing Antenna Corrections"
			fetchantenna(periodtocorr)
			os.chdir("fixinfo")
			antennaworkers=Pool(processes=6)
			for a in antenna_corrections:
				tocorrect=sorted(glob.glob(os.path.join("..",a,"*.dppp")))
				antennaworkers.map(fixantenna, tocorrect)
			print "Complete!"
			antennaworkers.close()
			os.chdir("..")
			open('ANTENNA_CORRECTIONS_PERFORMED','a').close()
			subprocess.call("rm -r fixinfo", shell=True)
		else:
			print "No Antenna Corrections Required"
		print "Removing tar files..."
		subprocess.call("rm -r *.tar", shell=True)
		print "Data Ready!"

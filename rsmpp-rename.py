#!/usr/bin/env python

import pyrap.tables as pt
import subprocess,glob,optparse,sys

usage = "usage: python %prog [options]"
description="Script to change the names of LOFAR dataset files"
vers='1.0'

#Defines the options
parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-f", "--force", action="store_true", dest="force", default=False, help="Ignore user input and just change the names from those found in the LOFAR table [default: %default]")
parser.add_option("-g", "--globpattern", action="store", type="string", dest="globpatt", default="*.MS", help="Choose the glob pattern for locating the datafiles in the current directory [default: %default]")
(options, args) = parser.parse_args()

def rename1(SB):
	SBtable=pt.table("{0}/OBSERVATION".format(SB), ack=False)
	newname=SBtable.col("LOFAR_FILENAME")[0]
	SBtable.close()
	if newname.endswith(".MS"):
		newname+=".dppp"
	return newname

def rename2(SB, obsid):
	SBtable=pt.table("{0}/OBSERVATION".format(SB), ack=False)
	beam=int(SBtable.col("LOFAR_SUB_ARRAY_POINTING")[0])
	SBtable.close()
	SBtable=pt.table("{0}/SPECTRAL_WINDOW".format(SB), ack=False)
	sbno=int(SBtable.col("NAME")[0].split("-")[-1])
	SBtable.close()
	newname="{0}_SAP{1:03d}_SB{2:03d}_uv.MS.dppp".format(obsid,beam,sbno)
	return newname
	

if __name__=="__main__":
	torename=sorted(glob.glob(options.globpatt))
	if len(torename)<1:
		print "There doesn't seem to be any datasets present matching the glob pattern {0}".format(options.globpatt)
		sys.exit()
	if not options.force:
		allow=['y','n']
		new_names={}
		for i in torename:
			new_names[i]=rename1(i)
			print "{0} --> {1}".format(i, new_names[i])
		answer_correct=False
		while not answer_correct:
			ok=raw_input("Are the new names sensible? [y/n]: ")
			if ok not in allow:
				print "Answer must be 'y' or 'n'."
			else:
				answer_correct=True
		if ok=="y":
			print "Renaming datasets..."
			for i in torename:
				subprocess.call(["mv",i,new_names[i]])
		else:
			print "Attempting second rename strategy..."
			new_names={}
			happy=0
			while happy!="y":
				obs=raw_input("Please enter the observation ID (including the 'L' eg. L107321): ")
				happy=raw_input("Is {0} correct? [y/n]: ".format(obs))
			for i in torename:
				new_names[i]=rename2(i,obs)
				print "{0} --> {1}".format(i, new_names[i])
			answer_correct=False
			while not answer_correct:
				ok=raw_input("Are the new names sensible? [y/n]: ")
				if ok not in allow:
					print "Answer must be 'y' or 'n'."
				else:
					answer_correct=True
			if ok=="y":
				print "Renaming datasets..."
				for i in torename:
					subprocess.call(["mv",i,new_names[i]])
			else:
				print "Sorry there's not much I can do..."
	else:
		new_names={}
		for i in torename:
			new_names[i]=rename1(i)
			print "{0} --> {1}".format(i, new_names[i])
			subprocess.call(["mv",i,new_names[i]])
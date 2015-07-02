#!/usr/bin/env python

import pyrap.tables as pt
import subprocess,glob,optparse,sys,os

usage = "usage: python %prog [options]"
description="Script to change the names of LOFAR dataset files"
vers='1.1'

#Defines the options
parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-f", "--force", action="store_true", dest="force", default=False, help="Ignore user input and just change the names from those found in the LOFAR table [default: %default]")
parser.add_option("-g", "--globpattern", action="store", type="string", dest="globpatt", default="*.MS", help="Choose the glob pattern for locating the datafiles in the current directory [default: %default]")
parser.add_option("--obsids", action="store_true", dest="obsids", default=False, help="Select to change the names of the observation ids to make sequential [default: %default]")
parser.add_option("--sort", action="store_true", dest="sort", default=False, help="Select to sort measurement sets into observation id named directories [default: %default]")
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
	
def renameobsids(torename):
	basename=torename[0]
	torename=torename[1:]
	newtargetobs=["L{0}".format(basename),]
	newnames={}
	for c in range(1,len(torename)+1):
		newnames[torename[c-1]]=basename+c
	print "WARNING - This will change the observations listed"
	for name in sorted(newnames):
		newname=newnames[name]
		print "Changing L{0} --> L{1}".format(name, newname)
	allow=['y','n']
	answer_correct=False
	while not answer_correct:
		ok=raw_input("Do you wish to continue? [y/n]: ")
		if ok not in allow:
			print "Answer must be 'y' or 'n'."
		else:
			answer_correct=True
	if not ok=="y":
		sys.exit()
	else:
		# for name in sorted(newnames):
		# 	newname=newnames[name]
		# 	strname="L{0}".format(newname)
		# 	if not os.path.isdir(strname):
		# 		os.mkdir(strname)
		# 	else:
		# 		print "New name {0} already exists! Will not overwrite.".format(strname)
		# 		print "Please manually sort data downloaded from the LTA and re-run the pipeline wit LTA fetch off."
		# 		sys.exit()
		for name in sorted(newnames):
			newname=newnames[name]
			print "Changing L{0} --> L{1}".format(name, newname)
			strname="L{0}".format(newname)
			if not os.path.isdir(strname):
                                os.mkdir(strname)
			filestochange=sorted(glob.glob("L{0}/*.dppp".format(name)))
			for file in filestochange:
				print file, file.replace(str(name), str(newname))
				subprocess.call("mv {0} {1}".format(file, file.replace(str(name), str(newname))), shell=True)
			try:
				os.rmdir("L{0}".format(name))
			except:
				print "L{0} doesn't appear to be empty, will not delete.".format(name)
			newtargetobs.append(strname)
		return newtargetobs
	
def sort(obs):
	dest=obs.split("_")[0]
	if not os.path.isdir(dest):
		os.mkdir(dest)
	newset=os.path.join(dest,obs)
	if not os.path.isdir(newset):
		subprocess.call("mv {0} {1}".format(obs, newset), shell=True)
		print "{0} moved to {1}".format(obs, dest)	
	else:
		print "{} already exists! Will not overwrite.".format(newset)
	
if __name__=="__main__":
	if options.obsids:
		torename=sorted(glob.glob(options.globpatt))
		if len(torename)<1:
			print "There doesn't seem to be any observation ID directories?".format(options.globpatt)
			sys.exit()
		else:
			torename_nums=[int(j.split("L")[-1]) for j in torename]
			renameobsids(torename_nums)
			print "Renaming finished."
	elif options.sort:
		tosort=sorted(glob.glob(options.globpatt))
		for i in tosort:
			sort(i)
	else:
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

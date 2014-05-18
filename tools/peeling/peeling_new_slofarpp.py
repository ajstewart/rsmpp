#! /usr/bin/env python

import optparse
import pyrap.tables as pt
import numpy as np
import math as m
import os
import scipy as sci
# Add a parser to the script

parser = optparse.OptionParser()

parser.add_option("-i", help="The input MS [required]", action="store", type="string", dest="input")
parser.add_option("-p", help="Parset for calibration [required]", action="store", type="string", dest="parset")
parser.add_option("-l", help="Flux limit in Jy down to which sources are included for peeling [10]", action="store", type="float", dest="limit", default=10.0)
parser.add_option("-f", help="Flux limit for the skymodel in Jy (only used for gsm.py) [0.1]", action="store", type="float", dest="flimit", default=0.1)
parser.add_option("-c", help="Radius around the pointing centre in which to search for sources in deg. (only used for gsm.py) [15]", action="store", type="float", dest="cone", default=15.0)
parser.add_option("-m", help="Sky model to use, e.g. if you have already run gsm.py and don't want to generate a new skymodel. If \'none\', gsm.py will be executed. [\'none\']", action="store", type="string", dest="model_fn", default="none")
parser.add_option("-v", help="Verbose mode, will print a bunch of helpful messages. [False]", action="store_true", default=False, dest="verbose")
parser.add_option("-n", help="The number of sources to be peeled. The flux limit is still preserved. If zero, peels all sources down to the flux limit. [0]", action="store", type="int", dest="nsources", default=0)
parser.add_option("-s", help="Explicitly specify sources to be peeled in the format of source1,source2,source3", action="store", type="string", dest="specsources", default="0")

options, arguments = parser.parse_args()

generate_skymodel = True
if options.model_fn.lower() != "none":
	generate_skymodel = False
	skymodel_fn = options.model_fn
else:
	skymodel_fn = "peeling.skymodel"


# Get the frequency and the pointing centre from the measurement set
sw = pt.table(options.input + '/SPECTRAL_WINDOW')
freq = sw.col('REF_FREQUENCY')[0]
sw.close()

# obs = pt.table(options.input + '/OBSERVATION')
# ra = float(obs.col('LOFAR_TARGET')[0][0].split()[1])
# dec = float(obs.col('LOFAR_TARGET')[0][0].split()[2])
# obs.close()

obs = pt.table(options.input + '/FIELD')
ra = float(obs.col('PHASE_DIR')[0][0][0])
if ra > 0.:
	ra*=(180./m.pi)
else:
	ra=360.+(ra*(180./m.pi))
dec = float(obs.col('PHASE_DIR')[0][0][1])*(180./m.pi)

# This isn't the FWHM, but the standard deviation (due to the 
# factor of 1/2.3548) to be used in the Gaussian tapering step later
fwhm = 1.1*((3.0e8/freq)/32.25)*180./m.pi/2.3548
if options.verbose:
	print "Primary beam FWHM (est. deg) = " + str(fwhm*2.3548)

# Calculate the skymodel from WENSS, VLSS and NVSS
if generate_skymodel:
	print 'Calculating skymodel!'
	os.system('gsm.py '+skymodel_fn+' ' + str(ra) + ' ' + str(dec) + ' ' + str(options.cone) + ' ' + str(options.flimit))

# Read in the sky model and create an array with ra,dec and flux. Adjust the sources to their apparent flux in the observation and find the sources abover an apparent limit to peel them.

tmp1 = []
tmp2 = []
tmp3 = []
tmpcs1 = []
tmpcs2 = []
tmpcs3 = []
tmpcs4 = []
brightest_name = ''
brightest_flux = 0


if options.verbose:
	print "RA = " + str(ra)
	print "Dec. = " + str(dec)
	print "***********************************************************"

for line in open(skymodel_fn):
	sline=line.split(',')
	if line[0] == '#': continue
	if line[0] == 'FORMAT': continue
	if line[0] == '\n': continue
	name = str(sline[0])
	ra_src = str(sline[2]).split(':')
	ra_deg = float(ra_src[0])*15.0 + (float(ra_src[1])/60.0)*15.0 + (float(ra_src[2])/3600.0)*15.0
	dec_src = str(sline[3]).split('.')
	if len(dec_src) == 3:
		dec_deg = float(dec_src[0]) + (float(dec_src[1])/60.0) + (float(dec_src[2])/3600.0)
	else:
		dec_deg = float(dec_src[0]) + (float(dec_src[1])/60.0) + (float(dec_src[2] + '.' + dec_src[3])/3600.0)
	flux = str(sline[4])
	dist = (180./m.pi)*m.acos(m.sin(dec*(m.pi/180.0)) * m.sin(dec_deg*(m.pi/180.0)) + m.cos(dec*(m.pi/180.0)) * m.cos(dec_deg*(m.pi/180.0)) * m.cos((ra-ra_deg)*(m.pi/180.0)))
	flux_corr = float(flux)*m.exp(-(dist**2)/2./fwhm**2)
	if flux_corr > brightest_flux:
		brightest_flux = flux_corr
		brightest_name = name

	if options.nsources == 0:

		if flux_corr > options.limit:
			if options.verbose:
				# print line
				print "Beam attenuated flux (est. Jy) = " + str(flux_corr)
				print "RA = " + str(ra_deg)
				print "Dec. = " + str(dec_deg)
				print "Dist. (Deg.) = " + str(dist)
				print "***********************************************************"
			tmp1.append(name)
			tmp2.append([name,dist])
		else:
			tmp3.append(name)
	else:	
		tmpcs2.append([name,flux_corr,ra_deg,dec_deg,dist])

names=[]
if options.nsources != 0:
	if options.specsources != "0":
		peel_names=options.specsources.split(",")
		for s in tmpcs2:
			if s[0] in peel_names and s[1] > options.limit:
				names.append(s[0])
				if options.verbose:
	                        	        print "Beam attenuated flux (est. Jy) = " + str(s[1])
	                                	print "RA = " + str(s[2])
	                               		print "Dec. = " + str(s[3])
	                                	print "Dist. (Deg.) = " + str(s[4])
	                                	print "***********************************************************"
			else:
				tmp3.append(s[0])
		
		print 'You are going to peel ' + str(len(names)) + ' sources!'
		peel_src = ",".join(names)
	else:
		# print "Got HERE"
		tmpcs = sorted(tmpcs2, key=lambda tdown: tdown[1], reverse=True)	#All sources sorted by flux
		# print tmpcs
		tmpcs3 = tmpcs[:options.nsources]		#sources to peel
		tmpcs4 = tmpcs[options.nsources:]		#rest of sources
		for i in tmpcs4:						#for each source in rest add name to tmp3
			tmp3.append(i[0])
		for l in tmpcs3:
			# print l						#for each source in to peel 
			if l[1] > options.limit:							
				names.append(l[0])				#if source over limit add name to names
				if options.verbose:
	                        	        print "Beam attenuated flux (est. Jy) = " + str(l[1])
	                                	print "RA = " + str(l[2])
	                               		print "Dec. = " + str(l[3])
	                                	print "Dist. (Deg.) = " + str(l[4])
	                                	print "***********************************************************"
			else:
				tmp3.append(l[0])				#if not add name to rest of sources

	
		print 'You are going to peel ' + str(len(names)) + ' sources!'
		peel_src = ",".join(names)
		#ref = tmpcs3[0][0]
else:
	print 'You are going to peel ' + str(len(tmp1)) + ' sources!'	#peel all sources above limit!

	peel_src = ",".join(tmp1)
	#tmp3 = sorted(tmp2, key=lambda tdown: tdown[1])
	#ref = tmp3[0][0]


other_src = ",".join(tmp3)
f = open(options.parset, 'rw')
newlines = []
for line in f:
    	if 'Step.add1.Model.Sources =' in line:
        	line = 'Step.add1.Model.Sources = [' + str(peel_src) + ']\n'
	if 'Step.solve.Model.Sources =' in line:
		line = 'Step.solve.Model.Sources = [' + str(peel_src) + ']\n'
	if 'Step.subtractstrong1.Model.Sources =' in line:
		line = 'Step.subtractstrong1.Model.Sources = [' + str(peel_src) + ']\n'
	if 'Step.add2.Model.Sources =' in line:
		line = 'Step.add2.Model.Sources = [' + str(other_src) + ']\n'
	if 'Step.solve2.Model.Sources =' in line:
		line = 'Step.solve2.Model.Sources = [' + str(other_src) + ']\n'
	#if 'Step.correct.Model.Sources =' in line:
	#	if not correct_brightest:
	#		line = 'Step.correct.Model.Sources = [' + str(ref) + ']\n'
	#	else:
	#		line = 'Step.correct.Model.Sources = [' + str(brightest_name) + ']\n'
	#if 'Step.add3.Model.Sources =' in line:
	#	line = 'Step.add3.Model.Sources = [' + str(peel_src) + ']\n'
		
    	newlines.append(line)

outfile = file(options.parset, 'w')
outfile.writelines(newlines)

# print 'Writing parset file!'
#if not correct_brightest:
#	if options.nsources != 0:
#		print 'The field will be corrected for ' + str(ref) + ', which is located ' + str(tmpcs3[0][4]) + ' degrees from the pointing centre!'
#	else:
#		print 'The field will be corrected for ' + str(ref) + ', which is located ' + str(tmp3[0][1]) + ' degrees from the pointing centre!'
#else:
#	print "The field will be corrected for " + str(brightest_name) + ", which has a beam attenuated flux of " + str(brightest_flux) + "Jy."
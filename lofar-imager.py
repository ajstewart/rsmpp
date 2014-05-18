#!/usr/bin/env python

import glob, subprocess, os, optparse, sys, pyfits, time
from functools import partial
from multiprocessing import Pool as mpl
import pyrap.tables as pt
import numpy as np
from datetime import datetime
from pyrap.quanta import quantity

usage = "usage: python %prog [options] $MSs/to/image "
description="A generic mass imaging script for LOFAR data using the AWimager. Takes care of naming, UV ranges, fits, masks, mosaicing and time split imaging.\
The data used must be in the format of 'L123456_SAP000_BAND01.MS.dppp'. Script originated from rsm_imager.py"
vers="6.0"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("--mask", action="store_true", dest="mask", default=False, help="Use option to use a mask when cleaning [default: %default]")
parser.add_option("-A", "--automaticthresh", action="store_true", dest="automaticthresh", default=False,help="Switch on automatic threshold method of cleaning [default: %default]")
parser.add_option("-I", "--initialiter", action="store", type="int", dest="initialiter", default=2500,help="Define how many cleaning iterations should be performed in order to estimate the threshold [default: %default]")
parser.add_option("-b", "--bandthreshs", action="store", type="string", dest="bandthreshs", default="0.053,0.038,0.035,0.028",help="Define the prior level of threshold to clean to for each band enter as '0.34,0.23,..' no spaces, in units of Jy [default: %default]")
parser.add_option("-u", "--maxbunit", action="store", type="string", dest="maxbunit", default="M",help="Choose which method to limit the baselines, enter 'UV' for UVmax (in klambda) or 'M' for physical length (in metres) [default: %default]")
parser.add_option("-l", "--maxbaseline", action="store", type="float", dest="maxbaseline", default=6000,help="Enter the maximum baseline to image out to, making sure it corresponds to the unit options [default: %default]")
parser.add_option("-m", "--mosaic", action="store_true", dest="mosaic", default=False, help="Also generate mosaics [default: %default]")
parser.add_option("-r", "--avgpbradius", action="store", type="float", dest="avgpbr", default=0.5, help="Radius beyond which to zero avgpb values (expressed as fraction of image width) [default: %default]")
parser.add_option("-N", "--NCPmos", action="store_true", dest="ncp", default=False, help="Use this option if mosaicing the NCP [default: %default]")
parser.add_option("-n", "--nice", action="store", type="int", dest="nice", default=5, help="Set the niceness level [default: %default]")
parser.add_option("-o", "--output", action="store", type="string", dest="output", default="images_standalone", help="Specify the name of the images folder that will hold the results. [default: %default]")
parser.add_option("-p", "--parset", action="store", type="string", dest="parset", default="aw.parset", help="Define parset to use containing AWimager options [default: %default]")
parser.add_option("-t", "--time", action="store", type="float", dest="time", default=-1.0, help="Select a time interval in which to image the datasets (in secs) [default: %default]")
parser.add_option("-w", "--overwrite", action="store_true", dest="overwrite", default=False, help="Select whether to overwrite previous results directory [default: %default]")
(options, args) = parser.parse_args()

def convert_newawimager(environ):
	"""
	Returns an environment that utilises the new version of the AWimager.
	"""
	new_envrion=environ
	environ['LOFARROOT']="/opt/share/lofar-archive/2013-02-11-16-46/LOFAR_r_b0fc3f4"
	environ['PATH']="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/opt/share/soft/pathdirs/bin:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/bin"
	environ['LD_LIBRARY_PATH']="/opt/share/soft/pathdirs/lib:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/lib"
	environ['PYTHONPATH']="/opt/share/soft/pathdirs/python-packages:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/python-packages"
	return new_envrion

def setup(out, maxunit, autothresh, bthreshs, oids, mos, msk):
	bandsthreshs_dict={}
	if checkdir(out):
		if options.overwrite:
			print "Overwrite option used, removing previous directory..."
			subprocess.call("rm -rf {0}".format(out), shell=True)
			if msk:
				os.makedirs(os.path.join(out, "masks"))
			else:
				os.mkdir(out)
		else:
			print "Overwrite option not used and directory '{0}' already exists.".format(out)
			return False, bandsthreshs_dict
		print "----------------------------------------------------------"
	else:
		if msk:
			os.makedirs(os.path.join(out, "masks"))
		else:
			os.mkdir(out)
	for o in oids:
		if mos:
			os.makedirs(os.path.join(out, o, "mosaics"))
			os.makedirs(os.path.join(out, o, "logs"))
		else:
			os.makedirs(os.path.join(out, o, "logs"))
	allowedunits=['UV', 'M']
	if maxunit not in allowedunits:
		print "Selected maximum baseline length unit is not valid"
		return False, bandsthreshs_dict
	if autothresh:
		tempbandsthresh=bthreshs.split(",")
		if len(tempbandsthresh) < 0:
			print "No band thresholds have been found!"
			return False, bandsthreshs_dict
		else:
			bandsthreshs_dict={}
			for i in range(0, len(tempbandsthresh)):
				bandsthreshs_dict["{0:02d}".format(i)]=float(tempbandsthresh[i])
	if len(toimage)<1:
		print "No images found matching input!"
		return False, bandsthreshs_dict
	return True, bandsthreshs_dict

def check_time(intvl):
	if intvl!=-1.0:
		if intvl<1.0:
			print "Cannot image in time steps of less than 1s"
			on="Error"
		else:
			print "Time Split Mode: ON"
			print "Going to image in {0} minute time intervals".format(round(intv/60., 4))
		on=True
	else:
		on=False
	print "----------------------------------------------------------"
	return on
	
def extract_parset(p, out, msk, mos, mxb):
	if not checkpres(p):
		print "Parset file {0} cannot be found".format(parset)
		aw_sets=False
	else:
		userthresh=0.0
		pad=1.0
		temp=open(p, "r")
		aw_sets=temp.readlines()
		temp.close()
		mask_size=""
		to_remove=[]
		for s in aw_sets:
			if s.startswith('#'):
				continue
			if "npix=" in s or "cellsize=" in s or "data=" in s:
				mask_size+=" "+s.strip('\n')
			if "niter=" in s:
				niters=int(s.split("=")[1])
				to_remove.append(s)
			if "threshold=" in s:
				userthresh=float(s.split("=")[1].replace("Jy", ""))
				to_remove.append(s)
			if "pad" in s:
				pad=float(s.split("=")[1])
		for j in to_remove:
			aw_sets.remove(j)
		print "Parset settings..."
		for a in aw_sets:
			print a.rstrip("\n")
		print "Maximum Baseline to image: {0} {1}".format(mxb, maxbunit)
		print "User threshold: {0}Jy".format(userthresh)
		print "User iterations: {0}".format(niters)
		if msk:
			print "Mask: On"
		else:
			print "Mask: Off"
		if mos:
			print "Mosaic: On"
		else:
			print "Mosaic: Off"
		print "----------------------------------------------------------"
		print "Your chance to cancel (Ctrl-c) if something is wrong!..."
		time.sleep(10)
		subprocess.call(["cp", p, os.path.join(out, "aw.parset_used")])
	return aw_sets, niters, userthresh, mask_size, pad
	
def check_msformat(mssets):
	print "Checking Measurement Sets to Image..."
	#store the information of ms to image as well as example MS
	bands={}
	beams={}
	obsids={}
	obsids_beams={}
	obsids_bands=[]
	# toremove=[]
	for m in mssets:
		name=m.split("/")[-1]
		if not name.startswith('L'):
			print "{0} does not follow expected naming".format(m)
			print "MS must be named firstly by the ID number eg. L123456"
			return False, beams, bands, obsids, obsids_beams
		if "_SAP" not in name or "_BAND" not in name:
			print "{0} does not follow expected naming".format(m)
			print "MS must have SAP and BAND information eg, SAP001 BAND02."
			return False, beams, bands, obsids, obsids_beams
		parts=name.split("_")
		obsid=parts[0]
		if obsid not in obsids:
			obsids[obsid]=m
			print obsid
		beam=int(name.split("SAP")[-1][:3])
		if beam not in beams:
			beams[beam]=m
		obsbeam="{0}_{1:03d}".format(obsid, beam)
		if obsbeam not in obsids_beams:
			obsids_beams[obsbeam]=m
			print "Beam: SAP{0:03d}".format(beam)
		band=int(name.split("BAND")[-1][:2])
		obsband="{0}_BAND{1:02d}".format(obsid, band)
		if band not in bands:
			bands[band]=m
		if obsband not in obsids_bands:
			ft = pt.table(m+'/SPECTRAL_WINDOW', ack=False)
			show_freq = ft.getcell('REF_FREQUENCY',0)
			ft.close()
			print "BAND{0:02d}: {1:00.02f} MHz".format(band, show_freq/1e6)
			obsids_bands.append(obsband)
	print "----------------------------------------------------------"
	return True, beams, bands, obsids, obsids_beams, obsids_bands

def getimgstd(infile):
	fln=pyfits.open(infile)
	rawdata=fln[0].data
	angle=fln[0].header['obsra']
	bscale=fln[0].header['bscale']
	rawdata=rawdata.squeeze()
	rawdata=rawdata*bscale
	while len(rawdata) < 20:
		rawdata = rawdata[0]
	X,Y = np.shape(rawdata)
	rawdata = rawdata[Y/6:5*Y/6,X/6:5*X/6]
	orig_raw = rawdata
	med, std, mask = Median_clip(rawdata, full_output=True, ftol=0.0, max_iter=10, sigma=3)
	rawdata[mask==False] = med
	fln.close()
	return std

def Median_clip(arr, sigma=3, max_iter=3, ftol=0.01, xtol=0.05, full_output=False, axis=None):
    """Median_clip(arr, sigma, max_iter=3, ftol=0.01, xtol=0.05, full_output=False, axis=None)
    Return the median of an array after iteratively clipping the outliers.
    The median is calculated upon discarding elements that deviate more than
    sigma * standard deviation the median.

    arr: array to calculate the median from.
    sigma (3): the clipping threshold, in units of standard deviation.
    max_iter (3): the maximum number of iterations. A value of 0 will
        return the usual median.
    ftol (0.01): fraction tolerance limit for convergence. If the number
        of discarded elements changes by less than ftol, the iteration is
        stopped.
    xtol (0.05): absolute tolerance limit for convergence. If the number
        of discarded elements increases above xtol with respect to the
        initial number of elements, the iteration is stopped.
    full_output (False): If True, will also return the indices that were good.
    axis (None): Axis along which the calculation is to be done. NOT WORKING!!!

    >>> med = Median_clip(arr, sigma=3, max_iter=3)
    >>> med, std, inds_good = Median_clip(arr, sigma=3, max_iter=3, full_output=True)
    """
    arr = np.ma.masked_invalid(arr)
    med = np.median(arr, axis=axis)
    std = np.std(arr, axis=axis)
    ncount = arr.count(axis=axis)
    for niter in xrange(max_iter):
        ncount_old = arr.count(axis=axis)
        if axis is not None:
            condition = (arr < np.expand_dims(med-std*sigma, axis)) + (arr > np.expand_dims(med+std*sigma, axis))
        else:
            condition = (arr < med-std*sigma) + (arr > med+std*sigma)
        arr = np.ma.masked_where(condition, arr)
        ncount_new = arr.count(axis)
        med = np.median(arr, axis=axis)
        std = np.std(arr, axis=axis)
        if np.any(ncount-ncount_new > xtol*ncount):
            print( "xtol reached {}; breaking at iteration {}".format(1-1.*ncount_new/ncount, niter+1) )
            break
        if np.any(ncount_old-ncount_new < ftol*ncount_old):
            print( "ftol reached {}; breaking at iteration {}".format(1-1.*ncount_new/ncount_old, niter+1) )
            break
    if full_output:
        if isinstance(arr.mask, np.bool_):
            mask = np.ones(arr.shape, dtype=bool)
        else:
            mask = ~arr.mask
        if axis is not None:
            med = med.data
            std = std.data
        return med, std, mask
    if axis is not None:
        med = med.data
    return med

def checkpres(file):
	if not os.path.isfile(file):
		return False
	else:
		return True

def checkdir(dir):
	if not os.path.isdir(dir):
		return False
	else:
		return True

def create_mask(todo, mask_size, out):
	for g in sorted(todo):
		info=g.split("_")
		id=info[0]
		beam="SAP{0}".format(info[-1])
		example=todo[g]
		print "Creating WENSS based model for {0}...".format(example)
		skymodel=os.path.join(out,"masks","{0}_{1}.skymodel".format(id, beam))
		subprocess.call("/home/as24v07/scripts/gsm_ms2.py -C wenss -A -r 5 {0} {1} > /dev/null 2>&1".format(example, skymodel), shell=True)
		mask=os.path.join(out,"masks","{0}_{1}.mask".format(id, beam))
		subprocess.call('makesourcedb in={0} out={0}.temp format=Name,Type,Ra,Dec,I,Q,U,V,ReferenceFrequency=\\\"60e6\\\",SpectralIndex=\\\"[0.0]\\\",MajorAxis,MinorAxis,Orientation > /dev/null 2>&1'.format(skymodel), shell=True)
		mask_command="awimager ms={0} image={1} operation=empty stokes='I'".format(example, mask)
		mask_command+=mask_size
		subprocess.call(mask_command+" > /dev/null 2>&1", shell=True)
		print "Creating {0} {1} mask...".format(id, beam)
		subprocess.call("/home/as24v07/scripts/msss_mask.py {0} {1}.temp > {2} 2>&1".format(mask, skymodel, os.path.join(out, "masks", g+".log")), shell=True)
		subprocess.call(["rm", "-r", "{0}.temp".format(skymodel)])

def create_mosaics(tomos, out, time_mode, avgpbrad, usencp):
	tomos_info=tomos.split("_")
	mos_obsid=tomos_info[0]
	mos_band=tomos_info[1]
	if time_mode:
		allimages=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.restored.corr".format(mos_band))))
		max_window=max([int(w.split("window")[-1][:3]) for w in allimages])
		for window in range(1, max_window+1):
			images=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*window{1:03d}*.restored.corr".format(mos_band, window))))
			avgpbs=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*window{1:03d}*.avgpb".format(mos_band, window))))
			for pb in avgpbs:
				print "Zeroing corners of avgpb {0}...".format(pb)
				subprocess.call("python /home/as24v07/scripts/avgpbz.py -r {0} {1}".format(avgpbrad, pb), shell=True)
			images_formatted=[j.replace(".restored.corr", "") for j in images]
			images_cmd=",".join(images_formatted)
			print "Creating Mosaic for {0} {1} Window {2}...".format(mos_obsid, mos_band, window)
			mosname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_window{2:03d}_mosaic.fits".format(mos_obsid, mos_band, window))
			sensname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_window{2:03d}_mosaic_sens.fits".format(mos_obsid, mos_band, window))
			subprocess.call("python /home/as24v07/scripts/mos.py -a avgpbz -o {0} -s {1} {2}".format(mosname, sensname, images_cmd), shell=True)
	else:
		images=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.restored.corr".format(mos_band))))
		avgpbs=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.avgpb".format(mos_band))))
		for pb in avgpbs:
			print "Zeroing corners of avgpb {0}...".format(pb)
			subprocess.call("python /home/as24v07/scripts/avgpbz.py -r {0} {1}".format(avgpbrad, pb), shell=True)
		images_formatted=[j.replace(".restored.corr", "") for j in images]
		images_cmd=",".join(images_formatted)
		print "Creating Mosaic for {0} {1}...".format(mos_obsid, mos_band)
		mosname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_mosaic.fits".format(mos_obsid, mos_band))
		sensname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_mosaic_sens.fits".format(mos_obsid, mos_band))
		if usencp:
			subprocess.call("python /home/as24v07/scripts/mos.py -N -a avgpbz -o {0} -s {1} -a avgpbz {2}".format(mosname, sensname, images_cmd), shell=True)
		else:
			subprocess.call("python /home/as24v07/scripts/mos.py -a avgpbz -o {0} -s {1} -a avgpbz {2}".format(mosname, sensname, images_cmd), shell=True)

def AW_Steps(g, usemask, aw_env, nit, maxb, initialiters, mosaic, automaticthresh, bandsthreshs_dict, uvORm, userthresh, padding, out, env):
	"""
	Performs imaging with AWimager using user supplied settings.
	"""
	c=299792458.
	if g.find("/"):
		logname=g.split("/")[-1]
	else:
		logname=g
	obsid=logname.split("_")[0]
	imagename=os.path.join(out, obsid, logname+".img")
	ft = pt.table(g+'/SPECTRAL_WINDOW', ack=False)
	freq = ft.getcell('REF_FREQUENCY',0)
	wave_len=c/freq
	if uvORm == "M":
		UVmax=maxb/(wave_len*1000.)
		localmaxb=maxb
	else:
		UVmax=maxb
		localmaxb=UVmax*(wave_len*1000.)
	ft.close()
	print "Wavelength = {0:00.02f} m / UVmax = {1}".format(wave_len, UVmax)
	beam=int(g.split("SAP")[1][:3])
	beamc="SAP00{0}".format(beam)
	finish_iters=nit
	if usemask:
		mask=os.path.join(out,"masks","{0}_{1}.mask".format(obsid, beamc))
	aw_parset_name="aw_{0}.parset".format(logname)
	if automaticthresh:
		curr_band=g.split("BAND")[1][:2]
		local_parset=open(aw_parset_name, 'w')
		local_parset.write("\nms={0}\n\
image={1}\n\
niter={2}\n\
threshold={3}Jy\n\
UVmax={4}\n".format(g, imagename, initialiters, 6.*bandsthreshs_dict[curr_band], UVmax))
		# if not nomask:
			# local_parset.write("mask={0}\n".format(mask))
		for i in aw_sets:
			local_parset.write(i)
		local_parset.close()
		print "Imaging {0} with AWimager...".format(g)
		subprocess.call("awimager {0} > {1}/{2}/logs/awimager_{3}_standalone_initial_log.txt 2>&1".format(aw_parset_name, out, obsid, logname), env=aw_env, shell=True)
		subprocess.call("image2fits in={0}.residual out={0}.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename, out, obsid), shell=True)
		try:
			thresh=2.5*(getimgstd("{0}.fits".format(imagename)))
		except:
			return
		# print "Cleaning {0} to threshold of {1}...".format(g, thresh)
		os.remove("{0}.fits".format(imagename))
	else:
		thresh=userthresh
	local_parset=open(aw_parset_name, 'w')
	local_parset.write("\nms={0}\n\
image={1}\n\
niter={2}\n\
threshold={3}Jy\n\
UVmax={4}\n".format(g, imagename, finish_iters, thresh, UVmax))
	if usemask:
		local_parset.write("mask={0}\n".format(mask))
	for i in aw_sets:
		local_parset.write(i)
	local_parset.close()
	print "Cleaning {0} to threshold of {1}...".format(g, thresh)
	subprocess.call("awimager {0} > {1}/{2}/logs/awimager_{3}_standalone_final_log.txt 2>&1".format(aw_parset_name, out, obsid, logname), env=aw_env, shell=True)
	if mosaic:
		subprocess.call("cp -r {0}.restored.corr {0}_mosaic.restored.corr".format(imagename), shell=True)
		subprocess.call("cp -r {0}0.avgpb {0}_mosaic0.avgpb".format(imagename), shell=True)
		if env=="rsm-mainline":
			if padding > 1.0:
				#we need to correct the avgpb for mosaicing
				print "Correcting {0} mosaic padding...".format(imagename)
				avgpb=pt.table("{0}_mosaic0.avgpb".format(imagename), ack=False, readonly=False)
				coordstable=avgpb.getkeyword('coords')
				coordstablecopy=coordstable.copy()
				value1=coordstablecopy['direction0']['crpix'][0]
				value2=coordstablecopy['direction0']['crpix'][1]
				value1*=padding
				value2*=padding
				newcrpix=np.array([value1, value2])
				coordstablecopy['direction0']['crpix']=newcrpix
				avgpb.putkeyword('coords', coordstablecopy)
				avgpb.close()
		subprocess.call("mv {0}*mosaic* {1}".format(imagename, os.path.join(out, obsid, "mosaics")), shell=True)
	subprocess.call("addImagingInfo {0}.restored.corr '' 0 {1} {2} > {3}/{4}/logs/addImagingInfo_standalone_{4}_log.txt 2>&1".format(imagename, localmaxb, g, out, obsid, logname), shell=True)
	subprocess.call("image2fits in={0}.restored.corr out={0}.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename, out, obsid), shell=True)
	os.remove(aw_parset_name)


def AW_Steps_split(g, interval, niter, aw_env, maxb, userthresh, uvORm, usemask, mosaic, padding, out, env):
	"""
	Performs imaging with AWimager using user supplied settings.
	"""
	c=299792458.
	tempgettime=pt.table(g, ack=False)
	ms_starttime=tempgettime.col("TIME")[0]#datetime.utcfromtimestamp(quantity(str(tempgettime.col("TIME")[0])+'s').to_unix_time())
	tempgettime.close()
	name=g.split("/")[-1]
	obsid=name.split("_")[0]
	home=os.path.join(out, obsid)
	temp=pt.table("{0}/OBSERVATION".format(g), ack=False)
	timerange=float(temp.col("TIME_RANGE")[0][1])-float(temp.col("TIME_RANGE")[0][0])
	temp.close()
	temp.done()
	ft = pt.table(g+'/SPECTRAL_WINDOW', ack=False)
	freq = ft.getcell('REF_FREQUENCY',0)
	wave_len=c/freq
	UVmax=maxb/(wave_len*1000.)
	ft.close()
	if uvORm == "M":
		UVmax=maxb/(wave_len*1000.)
		localmaxb=maxb
	else:
		UVmax=maxb
		localmaxb=UVmax*(wave_len*1000.)
	print "Wavelength = {0:00.02f} m / UVmax = {1}".format(wave_len, UVmax)
	beam=int(g.split("SAP")[1][:3])
	beamc="SAP00{0}".format(beam)
	if usemask:
		mask=os.path.join(out,"masks","{0}_{1}.mask".format(obsid, beamc))
	num_images=int(timerange/interval)
	interval_min=interval/60.
	interval_min_round=round(interval_min, 2)
	start_time=0.0
	end_time=interval_min_round
	for i in range(num_images):
		try:
			aw_parset_name="aw_{0}.parset".format(name)
			time_tag=".timesplit.{0:05.2f}min.window{1:03d}.img".format(interval_min, i+1)
			imagename=os.path.join(home, name+time_tag)
			imagename_short=name+time_tag
			logname=os.path.join(home,"logs",name+time_tag+".txt")
			thresh=userthresh
			print "Cleaning {0} Window {1:03d} to threshold of {2}...".format(name, i+1, thresh)
			local_parset=open(aw_parset_name, 'w')
			local_parset.write("\nms={0}\n\
image={1}\n\
niter={4}\n\
threshold={5}Jy\n\
t0={2}\n\
t1={3}\n\
UVmax={6}\n".format(g, imagename, start_time, end_time, niter, userthresh, UVmax))
			if usemask:
				local_parset.write("mask={0}\n".format(mask))
			for s in aw_sets:
				local_parset.write(s)
			local_parset.close()
			subprocess.call("awimager {0} > {1} 2>&1".format(aw_parset_name, logname), env=aw_env,  shell=True)
			if mosaic:
				subprocess.call("cp -r {0}.restored.corr {0}_mosaic.restored.corr".format(imagename), shell=True)
				subprocess.call("cp -r {0}0.avgpb {0}_mosaic0.avgpb".format(imagename), shell=True)
				if env=="rsm-mainline":
					if padding > 1.0:
						#we need to correct the avgpb for mosaicing
						print "Correcting {0} mosaic padding...".format(imagename)
						avgpb=pt.table("{0}_mosaic0.avgpb".format(imagename), ack=False, readonly=False)
						coordstable=avgpb.getkeyword('coords')
						coordstablecopy=coordstable.copy()
						value1=coordstablecopy['direction0']['crpix'][0]
						value2=coordstablecopy['direction0']['crpix'][1]
						value1*=padding
						value2*=padding
						newcrpix=np.array([value1, value2])
						coordstablecopy['direction0']['crpix']=newcrpix
						avgpb.putkeyword('coords', coordstablecopy)
						avgpb.close()
				subprocess.call("mv {0}*mosaic* {1}".format(imagename, os.path.join(out, obsid, "mosaics")), shell=True)
			subprocess.call("addImagingInfo {0}.restored.corr '' 0 {1} {2} > {3}/{4}/logs/addImagingInfo_standalone_{5}_log.txt 2>&1".format(imagename, localmaxb, g, out, obsid, imagename_short), shell=True)
			if start_time!=0.0:
				temp_change_start=pt.table("{0}.restored.corr/LOFAR_ORIGIN/".format(imagename), ack=False, readonly=False)
				temp_change_start2=pt.table("{0}.restored.corr/LOFAR_OBSERVATION/".format(imagename), ack=False, readonly=False)
				new_ms_startime=ms_starttime+start_time*60.
				temp_change_start.putcell('START',0,new_ms_startime)
				temp_change_start2.putcell('OBSERVATION_START',0,new_ms_startime)
				temp_change_start.close()
				temp_change_start2.close()
				if mosaic:
					mosaic_time=os.path.join(out, obsid, "mosaics", imagename_short+"_mosaic.restored.corr")
					tempimg=pt.table(mosaic_time, ack=False, readonly=False)
					restored_time=tempimg.getkeyword('coords')
					# print "restored_time: ", restored_time
					oldtime=restored_time['obsdate']['m0']['value']
					# print oldtime
					newtime=oldtime+((i*interval_min_round/(60.*24.)))
					# print newtime
					restored_time['obsdate']['m0']['value']=newtime
					tempimg.putkeyword('coords', restored_time)
					print "Changed Window {0:03d} Time to {1} (from {2})".format(i+1,datetime.utcfromtimestamp(quantity(str(newtime)+'d').to_unix_time()),datetime.utcfromtimestamp(quantity(str(oldtime)+'d').to_unix_time()))
					# print restored_time
					tempimg.close()
				# temp_inject.write("taustart_ts={0}".format(new_ms_startime.strftime("%Y-%m-%dT%H:%M:%S.0")))
			injparset="tkp_inject_{0}.parset".format(imagename_short)
			temp_inject=open(injparset, "w")
	 		temp_inject.write("tau_time={0}\n".format(interval))
			temp_inject.close()
			subprocess.call("trap-inject.py {0} {1}.restored.corr > {2}/{3}/logs/{4}_trapinject_log.txt 2>&1".format(injparset, imagename,out,obsid, imagename_short), shell=True)
			os.remove(injparset)
			os.remove(aw_parset_name)
			# print "image2fits in={0}.restored.corr out={0}.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename,out,obsid)
			subprocess.call("image2fits in={0}.restored.corr out={0}.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename,out,obsid), shell=True)
			start_time+=interval_min_round
			end_time+=interval_min_round
		except:
			start_time+=interval_min_round
			end_time+=interval_min_round
			continue

correct_lofarroot={'/opt/share/lofar-archive/2013-06-20-19-15/LOFAR_r23543_10c8b37':'rsm-mainline', 
'/opt/share/lofar/2013-09-30-16-27/LOFAR_r26772_1374418':'lofar-sept2013', 
'/opt/share/lofar/2014-01-22-15-21/LOFAR_r28003_357357b':'lofar-jan2014'
}

os.nice(options.nice)

usemask=options.mask
automaticthresh=options.automaticthresh
maxbunit=options.maxbunit.upper()
bandthreshs=options.bandthreshs
parset=options.parset
intv=options.time
output=options.output
mosaic=options.mosaic
maxb=options.maxbaseline
inititer=options.initialiter
avgpbr=options.avgpbr
ncp=options.ncp

#Check environment
curr_env=os.environ
if curr_env["LOFARROOT"] in correct_lofarroot:
	env=correct_lofarroot[curr_env["LOFARROOT"]]
else:
	env=curr_env["LOFARROOT"].split("/")[-2]

print "----------------------------------------------------------"
print "Running on '{0}' version of LOFAR software".format(env)
print "----------------------------------------------------------"
	
toimage=args[:]

#check images
toimage_ok, allbeams, allbands, allobsids, obsidsbeams, obsidsbands=check_msformat(toimage)
if not toimage_ok:
	print "----------------------------------------------------------"
	print "Error in checking measurement sets, see above message"
	print "----------------------------------------------------------"
	sys.exit()

#setup
ok, bandsthreshs_dict=setup(output, maxbunit, automaticthresh, bandthreshs, allobsids, mosaic, usemask)
if not ok:
	print "Setup failed, check above message"
	sys.exit()

#check time mode
time_mode=check_time(intv)
if time_mode=="Error":
	sys.exit()

#get parset info
aw_sets, niter, usrthresh, m_size, pad=extract_parset(parset, output, usemask, mosaic, maxb)
if aw_sets==False:
	sys.exit()
else:
	print "----------------------------------------------------------"
	print "Setup OK"

if env=="rsm-mainline":
	print "----------------------------------------------------------"
	print "Creating aw-imager environment as running on 'rsm-mainline'."
	awimager_environ=convert_newawimager(os.environ.copy())
else:
	awimager_environ=curr_env

#create masks
if usemask:
	create_mask(obsidsbeams, m_size, output)

AW_Steps_multi=partial(AW_Steps,
usemask=usemask, aw_env=awimager_environ, nit=niter,
maxb=maxb, initialiters=inititer,
mosaic=mosaic, automaticthresh=automaticthresh, bandsthreshs_dict=bandsthreshs_dict,
uvORm=maxbunit, userthresh=usrthresh, padding=pad, out=output, env=env)

AW_Steps_split_multi=partial(AW_Steps_split, interval=intv, niter=niter,
aw_env=awimager_environ, maxb=maxb, userthresh=usrthresh,
uvORm=maxbunit, usemask=usemask, mosaic=mosaic, padding=pad, out=output, env=env)

image_pool=mpl(processes=2)
if not time_mode:
	image_pool.map(AW_Steps_multi, toimage)
else:
	image_pool.map(AW_Steps_split_multi, toimage)

if mosaic:
	create_mosaics_multi=partial(create_mosaics, out=output, time_mode=time_mode, avgpbrad=avgpbr, usencp=ncp)
	image_pool.map(create_mosaics_multi, obsidsbands)

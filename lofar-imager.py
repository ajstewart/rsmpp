#!/usr/bin/env python

import glob, subprocess, os, optparse, sys, pyfits, time
from functools import partial
from multiprocessing import Pool as mpl
import pyrap.tables as pt
import numpy as np
from datetime import datetime
from pyrap.quanta import quantity

rootpath=os.path.realpath(__file__)
rootpath=rootpath.split("/")[:-1]
rootpath="/"+os.path.join(*rootpath)

tools={
"mosaicavgpb":os.path.join(rootpath, "tools", "mosaic", "avgpbz.py"),
"mosaic":os.path.join(rootpath, "tools", "mosaic", "mos.py"),
}

def convert_newawimager(environ):
	"""
	Returns an environment that utilises the new version of the AWimager. Used for RSM environment.
	"""
	new_envrion=environ
	environ['LOFARROOT']="/opt/share/lofar-archive/2013-02-11-16-46/LOFAR_r_b0fc3f4"
	environ['PATH']="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/opt/share/soft/pathdirs/bin:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/bin"
	environ['LD_LIBRARY_PATH']="/opt/share/soft/pathdirs/lib:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/lib"
	environ['PYTHONPATH']="/opt/share/soft/pathdirs/python-packages:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/python-packages"
	return new_envrion

def setup(out, autothresh, bthreshs, oids, mos, msk):
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
		# if mos:
		os.makedirs(os.path.join(out, o, "mosaics"))
		os.makedirs(os.path.join(out, o, "logs"))
		# else:
			# os.makedirs(os.path.join(out, o, "logs"))
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

def check_time(intvl, sonly, winds):
	if intvl!=-1.0:
		if intvl<1.0:
			print "Cannot image in time steps of less than 1s"
			on="Error"
		else:
			print "Time Split Mode: ON"
			if not sonly:
				print "Going to image in {0} second time intervals".format(intv)
				if winds!="all":
					print "Imaging windows {0} only".format(winds)
				else:
					print "Imaging all windows"
			else:
				print "Split only mode selected, will split ms into {0} second time intervals".format(intv)
		on=True
	else:
		on=False
		print "Time Split Mode: OFF"
	print "----------------------------------------------------------"
	return on
	
def extract_parset(p, out, msk, mos, mnb, mxb, bunit):
	if not checkpres(p):
		print "Parset file {0} cannot be found".format(parset)
		aw_sets=False
		return aw_sets, False, False, False, False
	else:
		userthresh=0.0
		pad=1.0
		temp=open(p, "r")
		aw_sets=temp.readlines()
		temp.close()
		mask_size=""
		to_remove=[]
		parsetminb=-1.0
		parsetmaxb=-1.0
		for s in aw_sets:
			if s.startswith('#'):
				continue
			elif "ms=" in s:
				to_remove.append(s)
			elif "image=" in s:
				to_remove.append(s)
			elif "npix=" in s or "cellsize=" in s or "data=" in s:
				mask_size+=" "+s.strip('\n')
			elif "niter=" in s:
				niters=int(s.split("=")[1])
				to_remove.append(s)
			elif "threshold=" in s:
				userthresh=float(s.split("=")[1].replace("Jy", ""))
				to_remove.append(s)
			elif "pad" in s:
				pad=float(s.split("=")[1])
			elif "UVmin=" in s:
				parsetminb=float(s.split("=")[1])
				to_remove.append(s)
			elif "UVmax=" in s:
				parsetmaxb=float(s.split("=")[1])
				to_remove.append(s)
		for j in to_remove:
			aw_sets.remove(j)
		if parsetminb!=-1.0:
			mnb=parsetminb
		if parsetmaxb!=-1.0:
			mxb=parsetmaxb
		print "Parset settings..."
		for a in aw_sets:
			print a.rstrip("\n")
		print "Minimum Baseline to image: {0} {1}".format(mnb, bunit)
		print "Maximum Baseline to image: {0} {1}".format(mxb, bunit)
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
		return aw_sets, niters, userthresh, mask_size, pad, mnb, mxb

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
		
def splitdataset(dataset, interval, out):
	name=dataset.split("/")[-1]
	print "Splitting {0} by {1} sec intervals...".format(name ,interval)
	t = pt.table(dataset, ack=False)
	starttime = t[0]['TIME']
	endtime   = t[t.nrows()-1]['TIME']
	numberofsplits=int((endtime-starttime)/interval)
	for split in range(0, numberofsplits):
		outputname=os.path.join(out, "splitMS", name+".{0}sec_{1:04d}.split".format(int(interval), split+1))
		if split==0:
			thisstart=starttime-2.
		else:
			thisstart=starttime+(float(split)*interval)
		thisend=starttime+((float(split)+1)*interval)
		t1 = t.query('TIME > ' + str(thisstart) + ' && \
		TIME < ' + str(thisend), sortlist='TIME,ANTENNA1,ANTENNA2')
		t1.copy(outputname, True)
		t1.close()
		if split==0:
			thisstart+=2.
		t1=pt.table(outputname+"/OBSERVATION", ack=False, readonly=False)
		thistimerange=np.array([thisstart, thisend])
		t1.putcell('TIME_RANGE', 0, thistimerange)
		t1.putcell('LOFAR_OBSERVATION_START', 0, thisstart)
		t1.putcell('LOFAR_OBSERVATION_END', 0, thisend)
		t1.close()
	t.close()

def create_mosaics(tomos, out, time_mode, avgpbrad, usencp):
	tomos_info=tomos.split("_")
	mos_obsid=tomos_info[0]
	mos_band=tomos_info[1]
	if time_mode:
		allimages=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.restored.corr".format(mos_band))))
		max_window=max([int(w.split(".split")[0].split("_")[-1]) for w in allimages])
		for window in range(1, max_window+1):
			images=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*_{1:04d}.split*.restored.corr".format(mos_band, window))))
			avgpbs=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*_{1:04d}.split*.avgpb".format(mos_band, window))))
			for pb in avgpbs:
				print "Zeroing corners of avgpb {0}...".format(pb)
				subprocess.call("{0} -r {1} {2}".format(tools["mosaicavgpb"], avgpbrad, pb), shell=True)
			images_formatted=[j.replace(".restored.corr", "") for j in images]
			images_cmd=",".join(images_formatted)
			print "Creating Mosaic for {0} {1} Window {2}...".format(mos_obsid, mos_band, window)
			mosname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_window{2:04d}_mosaic.fits".format(mos_obsid, mos_band, window))
			sensname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_window{2:04d}_mosaic_sens.fits".format(mos_obsid, mos_band, window))
			if usencp:
				subprocess.call("{0} -N -a avgpbz -o {1} -s {2} {3}".format(tools["mosaic"], mosname, sensname, images_cmd), shell=True)
			else:
				subprocess.call("{0} -a avgpbz -o {1} -s {2} {3}".format(tools["mosaic"], mosname, sensname, images_cmd), shell=True)
			correctedfits=os.path.join(out, mos_obsid, images_formatted[0].split("/")[-1].replace("_mosaic", "")+".restored.corr.fits")
			bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands=copyfitsinfo(correctedfits)
			correctfits(mosname, bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands)
	else:
		images=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.restored.corr".format(mos_band))))
		avgpbs=sorted(glob.glob(os.path.join(out, mos_obsid, "mosaics", "*{0}*.avgpb".format(mos_band))))
		for pb in avgpbs:
			print "Zeroing corners of avgpb {0}...".format(pb)
			subprocess.call("{0} -r {1} {2}".format(tools["mosaicavgpb"], avgpbrad, pb), shell=True)
		images_formatted=[j.replace(".restored.corr", "") for j in images]
		images_cmd=",".join(images_formatted)
		print "Creating Mosaic for {0} {1}...".format(mos_obsid, mos_band)
		mosname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_mosaic.fits".format(mos_obsid, mos_band))
		sensname=os.path.join(out, mos_obsid, "mosaics", "{0}_{1}_mosaic_sens.fits".format(mos_obsid, mos_band))
		if usencp:
			subprocess.call("{0} -N -a avgpbz -o {1} -s {2} -a avgpbz {3}".format(tools["mosaic"], mosname, sensname, images_cmd), shell=True)
		else:
			subprocess.call("{0} -a avgpbz -o {1} -s {2} -a avgpbz {3}".format(tools["mosaic"], mosname, sensname, images_cmd), shell=True)
		correctedfits=os.path.join(out, mos_obsid, images_formatted[0].split("/")[-1].replace("_mosaic", "")+".restored.corr.fits")
		bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands=copyfitsinfo(correctedfits)
		correctfits(mosname, bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands)
			
def open_subtables(table):
	"""open all subtables defined in the LOFAR format
	args:
	table: a pyrap table handler to a LOFAR CASA table
	returns:
	a dict containing all LOFAR CASA subtables
	"""
	subtable_names = (
	    'LOFAR_FIELD',
	    'LOFAR_ANTENNA',
	    'LOFAR_HISTORY',
	    'LOFAR_ORIGIN',
	    'LOFAR_QUALITY',
	    'LOFAR_STATION',
	    'LOFAR_POINTING',
	    'LOFAR_OBSERVATION'
	)
	subtables = {}
	for subtable in subtable_names:
		subtable_location = table.getkeyword("ATTRGROUPS")[subtable]
		subtables[subtable] = pt.table(subtable_location, ack=False)
	return subtables
	
def close_subtables(subtables):
	for subtable_name in subtables:
		subtables[subtable_name].close()
	return

def unique_column_values(table, column_name):
	"""
	Find all the unique values in a particular column of a CASA table.
	Arguments:
	- table:       ``pyrap.tables.table``
	- column_name: ``str``
	Returns:
	- ``numpy.ndarray`` containing unique values in column.
	"""
	return table.query(columns=column_name, sortlist="unique %s" % (column_name)).getcol(column_name)

def parse_subbands(subtables):
	origin_table = subtables['LOFAR_ORIGIN']
	num_chans = unique_column_values(origin_table, "NUM_CHAN")
	if len(num_chans) == 1:
		return num_chans[0]
	else:
		raise Exception("Cannot handle varying numbers of channels in image")

def parse_subbandwidth(subtables):
	# subband
	# see http://www.lofar.org/operations/doku.php?id=operator:background_to_observations&s[]=subband&s[]=width&s[]=clock&s[]=frequency
	freq_units = {
	'Hz': 1,
	'kHz': 10 ** 3,
	'MHz': 10 ** 6,
	'GHz': 10 ** 9,
	}
	observation_table = subtables['LOFAR_OBSERVATION']
	clockcol = observation_table.col('CLOCK_FREQUENCY')
	clock_values = unique_column_values(observation_table, "CLOCK_FREQUENCY")
	if len(clock_values) == 1:
		clock = clock_values[0]
		unit = clockcol.getkeyword('QuantumUnits')[0]
		trueclock = freq_units[unit] * clock
		subbandwidth = trueclock / 1024
		return subbandwidth
	else:
		raise Exception("Cannot handle varying clocks in image")


def parse_stations(subtables):
	"""Extract number of specific LOFAR stations used
	returns:
	(number of core stations, remote stations, international stations)
	"""
	observation_table = subtables['LOFAR_OBSERVATION']
	antenna_table = subtables['LOFAR_ANTENNA']
	nvis_used = observation_table.getcol('NVIS_USED')
	names = np.array(antenna_table.getcol('NAME'))
	mask = np.sum(nvis_used, axis=2) > 0
	used = names[mask[0]]
	ncore = nremote = nintl = 0
	for station in used:
		if station.startswith('CS'):
			ncore += 1
		elif station.startswith('RS'):
			nremote += 1
		else:
			nintl += 1
	return ncore, nremote, nintl

def getdatainfo(ms, imagename):
	t1=pt.table("{0}.restored.corr".format(imagename), ack=False)
	restbw=t1.getkeywords()['coords']['spectral2']['wcs']['cdelt']
	t1.close()
	t1=pt.table("{0}/OBSERVATION".format(ms), ack=False)
	thisendtime=t1.getcell('LOFAR_OBSERVATION_END', 0)
	thisantenna=t1.getcell('LOFAR_ANTENNA_SET', 0)
	t1.close()
	table = pt.table("{0}.restored.corr".format(imagename), ack=False)
	subtables = open_subtables(table)
	ncore, nremote, nintl =  parse_stations(subtables)
	subbandwidth = parse_subbandwidth(subtables)
	subbands = parse_subbands(subtables)
	close_subtables(subtables)
	return restbw, thisendtime, thisantenna, ncore, nremote, nintl, subbandwidth, subbands

def correctfits(fits_file, bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands):
	if type(endt)!=str:
		endtime=datetime.utcfromtimestamp(quantity(str(endt)+'s').to_unix_time())
		endtime=endtime.strftime("%Y-%m-%dT%H:%M:%S.%f")
	else:
		endtime=endt
	fits=pyfits.open(fits_file, mode="update")
	header=fits[0].header
	header.update('RESTBW',bw)
	header.update('END_UTC',endtime)
	header.update('ANTENNA',ant)
	header.update('NCORE',ncore)
	header.update('NREMOTE',nremote)
	header.update('NINTL',nintl)
	header.update('SUBBANDS',subbands)
	header.update('SUBBANDW',subbandwidth)
	fits.flush()
	fits.close()
	
def copyfitsinfo(fits_file):
	fits=pyfits.open(fits_file)
	header=fits[0].header
	bw=header['RESTBW']
	endt=header['END_UTC']
	ant=header['ANTENNA']
	ncore=header['NCORE']
	nremote=header['NREMOTE']
	nintl=header['NINTL']
	subbands=header['SUBBANDS']
	subbandwidth=header['SUBBANDW']
	fits.close()
	return bw, endt, ant, ncore, nremote, nintl, subbandwidth, subbands
	
def AW_Steps(g, usemask, aw_env, nit, minb, maxb, initialiters, mosaic, automaticthresh, bandsthreshs_dict, uvORm, userthresh, padding, out, env, time_mode, interval):
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
		UVmin=minb/(wave_len*1000.)
		UVmax=maxb/(wave_len*1000.)
		localmaxb=maxb
		localminb=minb
	else:
		UVmin=minb
		UVmax=maxb
		localminb=UVmin*(wave_len*1000.)
		localmaxb=UVmax*(wave_len*1000.)
	ft.close()
	# print "Wavelength = {0:00.02f} m / UVmin = {2}, UVmax = {1}".format(wave_len, UVmax, UVmin)
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
UVmin={5}\n\
UVmax={4}\n".format(g, imagename, initialiters, 6.*bandsthreshs_dict[curr_band], UVmax, UVmin))
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
			return g
		# print "Cleaning {0} to threshold of {1}...".format(g, thresh)
		os.remove("{0}.fits".format(imagename))
	else:
		thresh=userthresh
	local_parset=open(aw_parset_name, 'w')
	local_parset.write("\nms={0}\n\
image={1}\n\
niter={2}\n\
threshold={3}Jy\n\
UVmin={5}\n\
UVmax={4}\n".format(g, imagename, finish_iters, thresh, UVmax, UVmin))
	if usemask:
		local_parset.write("mask={0}\n".format(mask))
	for i in aw_sets:
		local_parset.write(i)
	local_parset.close()
	print "Cleaning {0} to threshold of {1}...".format(logname, thresh)
	subprocess.call("awimager {0} > {1}/{2}/logs/awimager_{3}_standalone_final_log.txt 2>&1".format(aw_parset_name, out, obsid, logname), env=aw_env, shell=True)
	if os.path.isdir("{0}.restored.corr".format(imagename)):
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
		subprocess.call("addImagingInfo {0}.restored '' {6} {1} {2} > {3}/{4}/logs/addImagingInfo_standalone_{5}_log.txt 2>&1".format(imagename, localmaxb, g, out, obsid, logname, localminb), shell=True)
		subprocess.call("addImagingInfo {0}.restored.corr '' {6} {1} {2} > {3}/{4}/logs/addImagingInfo_standalone_{5}_log.txt 2>&1".format(imagename, localmaxb, g, out, obsid, logname, localminb), shell=True)
		subprocess.call("image2fits in={0}.restored out={0}.restored.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename, out, obsid), shell=True)
		subprocess.call("image2fits in={0}.restored.corr out={0}.restored.corr.fits > {1}/{2}/logs/image2fits.log 2>&1".format(imagename, out, obsid), shell=True)
		#Correct fits - need to add RESTBW and end time
		#Getting RESTBW
		# t1=pt.table("{0}.restored.corr".format(imagename), ack=False)
# 		restbw=t1.getkeywords()['coords']['spectral2']['wcs']['cdelt']
# 		t1.close()
# 		t1=pt.table("{0}/OBSERVATION".format(g), ack=False)
# 		thisendtime=t1.getcell('LOFAR_OBSERVATION_END', 0)
# 		thisantenna=t1.getcell('LOFAR_ANTENNA_SET', 0)
# 		t1.close()
# 		table = pt.table("{0}.restored.corr".format(imagename), ack=False)
# 		subtables = open_subtables(table)
# 		ncore, nremote, nintl =  parse_stations(subtables)
# 		subbandwidth = parse_subbandwidth(subtables)
# 		subbands = parse_subbands(subtables)
# 		close_subtables(subtables)
		restbw, thisendtime, thisantenna, ncore, nremote, nintl, subbandwidth, subbands=getdatainfo(g, imagename)
		fitstofix=["{0}.restored.corr.fits".format(imagename), "{0}.restored.fits".format(imagename)]
		for fix in fitstofix:
			correctfits(fix, restbw, thisendtime, thisantenna, ncore, nremote, nintl, subbandwidth, subbands)
	else:
		print "{0} failed to image.".format(logname)
		return g
	os.remove(aw_parset_name)
	return "Done"


usage = "usage: python %prog [options] $MSs/to/image "
description="A generic mass imaging script for LOFAR data using the AWimager. Takes care of naming, UV ranges, fits, masks, mosaicing and time split imaging.\
The data used must be in the format of 'L123456_SAP000_BAND01.MS.dppp'. Script originated from rsm_imager.py"
vers="7.1"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("--mask", action="store_true", dest="mask", default=False, help="Use option to use a mask when cleaning [default: %default]")
parser.add_option("-A", "--automaticthresh", action="store_true", dest="automaticthresh", default=False,help="Switch on automatic threshold method of cleaning [default: %default]")
parser.add_option("-I", "--initialiter", action="store", type="int", dest="initialiter", default=2500,help="Define how many cleaning iterations should be performed in order to estimate the threshold [default: %default]")
parser.add_option("-b", "--bandthreshs", action="store", type="string", dest="bandthreshs", default="0.053,0.038,0.035,0.028",help="Define the prior level of threshold to clean to for each band enter as '0.34,0.23,..' no spaces, in units of Jy [default: %default]")
parser.add_option("-u", "--maxbunit", action="store", type="choice", dest="maxbunit", choices=['UV', 'M'], default="UV",help="Choose which method to limit the baselines, enter 'UV' for UVmax (in klambda) or 'M' for physical length (in metres) [default: %default]")
parser.add_option("-k", "--minbaseline", action="store", type="float", dest="minbaseline", default=0.0,help="Enter the maximum baseline to image out to, making sure it corresponds to the unit options [default: %default]")
parser.add_option("-l", "--maxbaseline", action="store", type="float", dest="maxbaseline", default=3.0,help="Enter the maximum baseline to image out to, making sure it corresponds to the unit options [default: %default]")
parser.add_option("-m", "--mosaic", action="store_true", dest="mosaic", default=False, help="Also generate mosaics [default: %default]")
parser.add_option("-r", "--avgpbradius", action="store", type="float", dest="avgpbr", default=0.5, help="Radius beyond which to zero avgpb values (expressed as fraction of image width) [default: %default]")
parser.add_option("-N", "--NCPmos", action="store_true", dest="ncp", default=False, help="Use this option if mosaicing the NCP [default: %default]")
parser.add_option("-n", "--nice", action="store", type="int", dest="nice", default=5, help="Set the niceness level [default: %default]")
parser.add_option("-o", "--output", action="store", type="string", dest="output", default="images_standalone", help="Specify the name of the images folder that will hold the results. [default: %default]")
parser.add_option("-p", "--parset", action="store", type="string", dest="parset", default="aw.parset", help="Define parset to use containing AWimager options [default: %default]")
parser.add_option("-t", "--time", action="store", type="float", dest="time", default=-1.0, help="Select a time interval in which to image the datasets (in secs) [default: %default]")
parser.add_option("-W", "--windows", action="store", type="string", dest="windows", default="all", help="Select specific time windows to image only separated by a comma e.g. in the form '1,6,17,21' [default: %default]")
parser.add_option("--splitMSonly", action="store_true", dest="splitonly", default=False, help="Select to simply perform the splitting of the chosen MS in time only with no imaging [default: %default]")
parser.add_option("--keepsplitMS", action="store_true", dest="keepsplit", default=False, help="Select to keep the split MS files that are produced. Otherwise these are deleted. [default: %default]")
parser.add_option("-w", "--overwrite", action="store_true", dest="overwrite", default=False, help="Select whether to overwrite previous results directory [default: %default]")
(options, args) = parser.parse_args()

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
minb=options.minbaseline
inititer=options.initialiter
avgpbr=options.avgpbr
ncp=options.ncp
keepsplit=options.keepsplit
splitonly=options.splitonly
windows=options.windows

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
ok, bandsthreshs_dict=setup(output, automaticthresh, bandthreshs, allobsids, mosaic, usemask)
if not ok:
	print "Setup failed, check above message"
	sys.exit()

#check time mode
if windows != "all":
	windows=[int(w) for w in windows.split(",")]
time_mode=check_time(intv, splitonly, windows)
if time_mode=="Error":
	sys.exit()

#get parset info
aw_sets, niter, usrthresh, m_size, pad, minb, maxb=extract_parset(parset, output, usemask, mosaic, minb, maxb, maxbunit)
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
minb=minb, maxb=maxb, initialiters=inititer,
mosaic=True, automaticthresh=automaticthresh, bandsthreshs_dict=bandsthreshs_dict,
uvORm=maxbunit, userthresh=usrthresh, padding=pad, out=output, env=env, time_mode=time_mode, interval=intv)

# AW_Steps_split_multi=partial(AW_Steps_split, interval=intv, niter=niter,
# aw_env=awimager_environ, minb=minb, maxb=maxb, userthresh=usrthresh,
# uvORm=maxbunit, usemask=usemask, mosaic=True, padding=pad, out=output, env=env)

if time_mode:
	os.mkdir(os.path.join(output, "splitMS"))
	split_workers=mpl(processes=6)
	splitdataset_multi=partial(splitdataset, interval=intv, out=output)
	split_workers.map(splitdataset_multi, toimage)
	split_workers.close()
	if splitonly:
		print "Split only option selected, now exiting."
		sys.exit()
	if windows=="all":
		toimage=sorted(glob.glob(os.path.join(output, "splitMS", "*.MS.*")))
	else:
		toimage=[]
		for w in windows:
			toimage+=sorted(glob.glob(os.path.join(output, "splitMS", "*.MS.*sec_{0:04d}.split".format(w))))

if not os.path.isdir("JAWS_products"):
    os.mkdir("JAWS_products")
image_pool=mpl(processes=2)
# if not time_mode:
failed=image_pool.map(AW_Steps_multi, toimage)

failed=[j for j in failed if j != "Done"]

if len(failed) > 0:
	print "Imaging failed images (one at a time)..."
	failed_pool=mpl(processes=1)
	failed_pool.map(AW_Steps_multi, failed)
	failed_pool.close()
# else:
	# image_pool.map(AW_Steps_split_multi, toimage)

if mosaic:
	create_mosaics_multi=partial(create_mosaics, out=output, time_mode=time_mode, avgpbrad=avgpbr, usencp=ncp)
	image_pool.map(create_mosaics_multi, obsidsbands)
	
if time_mode:
	if not keepsplit:
		print "Deleting Split MS files..."
		subprocess.call("rm -rf {0}".format(os.path.join(output, "splitMS")), shell=True)

image_pool.close()
os.rmdir("JAWS_products")

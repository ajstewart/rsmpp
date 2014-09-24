#Version 2.00a

import os, subprocess,time, multiprocessing, glob, datetime, pyfits, logging, sys
import numpy as np
import pyrap.tables as pt
from collections import Counter

rootpath=os.path.realpath(__file__)
rootpath=rootpath.split("/")[:-1]
rootpath="/"+os.path.join(*rootpath)

tools={"editparmdb":os.path.join(rootpath, "tools", "edit_parmdb", "edit_parmdb.py"),
"solplot":os.path.join(rootpath, "tools", "plotting", "solplot.py"),
"concat":os.path.join(rootpath, "tools", "concat.py"),
"peelingparsets":os.path.join(rootpath, "tools", "peeling", "parsets"),
"peelingnew":os.path.join(rootpath, "tools", "peeling", "peeling_new_slofarpp.py"),
"peelingfloat":os.path.join(rootpath, "tools", "peeling", "float_solutions.py"),
"msssmask":os.path.join(rootpath, "tools", "msss_mask.py"), 
"average":os.path.join(rootpath, "tools", "average_inverse_var3.py"),
"mosaicavgpb":os.path.join(rootpath, "tools", "mosaic", "avgpbz.py"),
"mosaic":os.path.join(rootpath, "tools", "mosaic", "mos.py"),
"ascii":os.path.join(rootpath, "tools", "plotting", "asciistats.py"),
"stats":os.path.join(rootpath, "tools", "plotting", "statsplot.py"),
"HBAdefault":os.path.join(rootpath, "tools", "HBAdefault"),
"LBAdefault":os.path.join(rootpath, "tools", "LBAdefault"),
}

log=logging.getLogger("rsm")

class Ddict(dict):
	def __init__(self, default=None):
		self.default = default

	def __getitem__(self, key):
		if not self.has_key(key):
			self[key] = self.default()
		return dict.__getitem__(self, key)

def fetch(file):
	"""Simple wget get line"""
	log.info("Fetching {0}...".format(file.split("/")[-1]))
	subprocess.call("wget {0} > /dev/null 2>&1".format(file), shell=True)

def fetchgrid(file):
	"""Simple wget get line"""
	log.info("Fetching {0}...".format(file.split("/")[-1]))
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

def clean(f):
	"""Function to 'clean' a sky model. It removes double sources, A-team sources and replaces MSSS calibrators with MSSS calibrator models."""
	Ateam=["2323.2+5850", "2323.4+5849", "1959.4+4044"]
	calibrators={"1411.3+5212":"3c295A, POINT, 14:11:20.49, +52.12.10.70, 48.8815, , , , 150e6, [-0.582, -0.298, 0.583, -0.363]\n3c295B, POINT, 14:11:20.79, +52.12.07.90, 48.8815, , , , 150e6, [-0.582, -0.298, 0.583, -0.363]\n",
	"0542.6+4951":"3c147, POINT, 05:42:36.1, 49.51.07, 66.738, , , , 150e6 , [-0.022, -1.012, 0.549]\n",
	"0813.6+4813":"3c196, POINT, 08:13:36.0, 48.13.03, 83.084, , , , 150e6, [-0.699, -0.110]\n",
	"1331.1+3030":"3c286, POINT, 13:31:08.3, 30.30.33, 27.477, , , , 150e6, [-0.158, 0.032, -0.180]\n",
	"1330.6+2509":"3c287, POINT, 13:30:37.7, 25.09.11, 16.367, , , , 150e6, [-0.364]\n",
	"1829.5+4844":"3c380, POINT, 18:29:31.8, 48.44.46, 77.352, , , , 150e6, [-0.767]\n",
	"0137.6+3309":"3c48,  POINT, 01:37:41.3, 33.09.35, 64.768, , [-0.387, -0.420, 0.181]\n"}
	input_model=open(f+".temp", 'r')
	output_model=open(f, 'w+r')
	source_names=[]
	for line in input_model:
		source=str(line)
		source_name=source[:11]
		# print source_name
		if source_name not in source_names:
			if source_name in Ateam:
				log.info("A team source {0} removed".format(source_name))
				continue
			if source_name in calibrators:
				source=calibrators[source_name]
				newsourcename=source.split(",")[0]
				log.info("{0} detected in target field - replaced gsm.py with MSSS component.".format(newsourcename))
			source_names.append(source_name)
			output_model.write(source)
		else:
			log.warning("Source {0} was doubled - removed second copy".format(source_name))
	input_model.close()
	output_model.close()
	log.info("Cleaned sky model {0} produced".format(f))
	subprocess.call("rm {0}.temp".format(f), shell=True)
	
def create_model(ms, outfile, rad):
	cut=0.1
	asth=0.00278
	log.info("Obtaining RA and Dec of {0}...".format(ms))
	obs = pt.table(ms + '/FIELD', ack=False)
	ra = np.degrees(float(obs.col('REFERENCE_DIR')[0][0][0]))
	if ra < 0.:
		ra=360.+(ra)
	dec = np.degrees(float(obs.col('REFERENCE_DIR')[0][0][1]))
	log.info("RA:{0}\tDec:{1}".format(ra, dec))
	obs.close()
	subprocess.call("gsm.py {0}.temp {1} {2} {3} {4} {5} > /dev/null 2>&1".format(outfile, ra, dec, rad, cut, asth), shell=True)
	clean(outfile)
	
def NDPPP_Initial(SB, wk_dir, ndppp_base, prec, precloc):
	"""
	Creates an NDPPP parset file using settings already supplied and adds\
	the msin and out parameters. Then runs using NDPPP and removes the parset.
	"""
	curr_SB=SB.split('/')[-1]
	curr_obs=curr_SB.split("_")[0]
	ndppp_filename='ndppp.initial.{0}.parset'.format(curr_SB)
	g = open(ndppp_filename, 'w')
	g.write("msin={0}\n".format(SB))
	if prec:
		g.write("msin.datacolumn = {0}\n".format(precloc))
		if SB[-3:]==".MS":
			g.write("msout={0}.dppp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
		else:	
			g.write("msout={0}\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
	else:
		g.write("msin.datacolumn = DATA\n")
		if SB[-3:]==".MS":
			g.write("msout={0}.dppp.tmp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
		else:	
			g.write("msout={0}.tmp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
	for i in ndppp_base:
		g.write(i)
	g.close()
	log.info("Performing Initial NDPPP on {0}...".format(curr_SB))
	subprocess.call("NDPPP {0} > {1}/logs/ndppp.{2}.log 2>&1".format(ndppp_filename, curr_obs, curr_SB), shell=True)
	os.remove(ndppp_filename)
	
def rficonsole(ms,mode, obsid):
	if mode=="LBA":
		strategy=tools["LBAdefault"]
	else:
		strategy=tools["HBAdefault"]
	log.info("Running rficonsole on {0}...".format(ms))
	subprocess.call("rficonsole -j 1 -strategy {0} {1} > {2}/logs/rficonsole.{3}.log 2>&1".format(strategy, ms, obsid, ms.split("/")[-1]), shell=True)

def check_dataset(ms):
	check=pt.table(ms, ack=False)
	try:
		row=check.row("DATA").get(0)
	except:
		log.warning("{0} is corrupt!".format(ms))
		return ms
	else:
		check.close()
		return True
		
def shiftndppp(target, tar_obs, target_name):
	"""
	Simply shifts the CORRECTED_DATA to a new measurement set DATA column.
	"""
	shift_ndppp=open("ndppp.shift_{0}.parset".format(target_name), 'w')
	shift_ndppp.write("msin={0}\n\
# msin.missingdata=true\n\
# msin.orderms=false\n\
msin.datacolumn=CORRECTED_DATA\n\
msin.baseline=*&\n\
msout={1}\n\
msout.datacolumn=DATA\n\
steps=[]".format(target, target.replace(".dppp.tmp", ".dppp")))
	shift_ndppp.close()
	log.info("Performing shift NDPPP for {0}...".format(target_name))
	subprocess.call("NDPPP ndppp.shift_{0}.parset > {1}/logs/ndppp_shift_{0}.log 2>&1".format(target_name, tar_obs), shell=True)
	os.remove("ndppp.shift_{0}.parset".format(target_name))
	if os.path.isdir(target.replace(".dppp.tmp", ".dppp")):
		subprocess.call("rm -r {0}".format(target), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)

def rsm_bandsndppp(a, rsm_bands):
	"""
	Function to combine together the sub bands into bands.
	"""
	info=a.split("_")
	current_obs=info[0]
	beamc=info[1]
	b=current_obs+"_"+beamc
	band=int(info[2])
	# b_real=b+(beam*34)
	log.info("Combining {0} BAND{1}...".format(b, '%02d' % band))
	filename="{0}_ndppp.band{1}.parset".format(b, '%02d' % band)
	n=open(filename, "w")
	n.write("msin={0}\n\
msin.datacolumn=DATA\n\
msin.baseline=[CR]S*&\n\
msin.missingdata=True\n\
msin.orderms=False\n\
msout={1}/{2}_BAND{3}.MS.dppp.tmp\n\
steps=[]".format(rsm_bands[a], current_obs, b,'%02d' % band))
	n.close()
	subprocess.call("NDPPP {0} > {1}/logs/{2}_BAND{3}.log 2>&1".format(filename,current_obs,b,'%02d' % band), shell=True)
	os.remove(filename)
	
def calibrate_msss2(target, phaseparset, flag, toflag, autoflag, create_sky, skymodel):
	"""
	Function for the second half of MSSS style calibration - it performs a phase-only calibration and the auto flagging \
	if selected.
	"""
	tsplit=target.split("/")
	curr_obs=tsplit[0]
	name=tsplit[-1]
	beam=target.split("_")[1]
	if create_sky==True:
		skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Performing phase only calibration on {0}...".format(target))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {3}/logs/calibrate_phase_{4}.txt 2>&1".format(target, phaseparset, skymodel, curr_obs, name), shell=True)
	if flag==True:
		local_toflag=toflag
		if autoflag==True:
			final_toflag=flagging(target, local_toflag)[:-1]
		log.info("Flagging baselines: {0} from {1}".format(final_toflag, target))
	else:
		final_toflag=""
	subprocess.call('msselect in={0} out={1} baseline=\'{2}\' deep=true > {3}/logs/msselect.log 2>&1'.format(target, target.replace(".tmp", ""), final_toflag, curr_obs), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)
	if os.path.isdir(target.replace(".tmp", "")):
		subprocess.call("rm -rf {0}".format(target), shell=True)


def standalone_phase(target, phaseparset, flag, toflag, autoflag, create_sky, skymodel, phaseoutput, phasecolumn):
	"""
	Simply shifts the CORRECTED_DATA to a new measurement set DATA column.
	"""
	tsplit=target.split("/")
	target_name=tsplit[-1]
	curr_obs=tsplit[0]
	beam=target.split("_")[1]
	phase_shift_ndppp=open("ndppp.shift_{0}.parset".format(target_name), 'w')
	phase_shift_ndppp.write("msin={0}\n\
msin.datacolumn={1}\n\
msin.baseline=*&\n\
msout={0}.PHASEONLY.tmp\n\
msout.datacolumn=DATA\n\
steps=[]".format(target, phasecolumn))
	phase_shift_ndppp.close()
	log.info("Performing phase shift NDPPP for {0}...".format(target_name))
	subprocess.call("NDPPP ndppp.shift_{0}.parset > {1}/logs/ndppp_phase_standalone_shift_{0}.log 2>&1".format(target_name, curr_obs), shell=True)
	os.remove("ndppp.shift_{0}.parset".format(target_name))
	target+=".PHASEONLY.tmp"
	if create_sky:
		skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Performing phase only calibration on {0}...".format(target))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {3}/logs/calibrate_standalone_phase_{4}.txt 2>&1".format(target, phaseparset, skymodel, curr_obs, target_name), shell=True)
	if flag:
		local_toflag=toflag
		if autoflag:
			final_toflag=flagging(target, local_toflag)[:-1]
		log.info("Flagging baselines: {0} from {1}".format(final_toflag, target))
	else:
		final_toflag=""
	subprocess.call('msselect in={0} out={1} baseline=\'{2}\' deep=true > {3}/logs/msselect_phaseonly.log 2>&1'.format(target, os.path.join(curr_obs, phaseoutput,target_name+".PHASEONLY"),final_toflag,curr_obs), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)
	if os.path.isdir(os.path.join(curr_obs, phaseoutput,target_name+".PHASEONLY")):
		subprocess.call("rm -rf {0}".format(target), shell=True)
	subprocess.call("mv {0}*.pdf {0}*.stats {0}*.tab {1}/flagging/".format(target, curr_obs), shell=True)


def flagging(target, local_toflag):
	"""
	A function which copies the auto detection of bad stations developed during MSSS.
	"""
	log.info("Gathering AutoFlag Information for {0}...".format(target))
	subprocess.call('{0} -i {1} -r {2}/ > {2}/logs/asciistats.log 2>&1'.format(tools["ascii"], target, target.split("/")[0]), shell=True)
	subprocess.call('{0} -i {1}.stats -o {1} > logs/statsplot.log 2>&1'.format(tools["stats"], target), shell=True)
	stats=open('{0}.tab'.format(target), 'r')
	for line in stats:
		if line.startswith('#')==False:
			cols=line.rstrip('\n').split('\t')
			if cols[12] == 'True':
				if cols[12] not in local_toflag:
					local_toflag+=('!'+cols[1]+';')
	return local_toflag

def peeling_steps(SB, shortpeel, peelsources, peelnumsources, fluxlimit, skymodel, create_sky):
	"""
	Performs the peeling steps developed during MSSS activities.
	"""
	peelsplit=SB.split('/')
	logname=peelsplit[-1]
	obsid=peelsplit[0]
	prepeel=logname+".prepeel"
	beam=logname.split("_")[1]
	if create_sky:
		skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Creating new {0} dataset ready for peeling...".format(SB))
	p_shiftname="peeling_shift_{0}.parset".format(logname)
	f=open(p_shiftname, 'w')
	f.write("msin={0}\n\
msin.datacolumn=CORRECTED_DATA\n\
msout={0}.peeltmp\n\
steps=[]".format(SB))
	f.close()
	subprocess.call("NDPPP {0} > {1}/logs/ndppp_peeling_shift_{2}.log 2>&1".format(p_shiftname, obsid, logname), shell=True)
	peelparset=SB+"_peeling.parset"
	if shortpeel:
		log.info("Performing only first stage of peeling (i.e. peeled sources will not be re-added)")
		subprocess.call(['cp', "{0}".format(os.path.join(tools["peelingparsets"],'peeling_new.parset')), peelparset])
	else:
		log.info("Performing full peeling steps")
		peel2parset=SB+'_peeling_step2.parset'
		subprocess.call(['cp', "{0}".format(os.path.join(tools["peelingparsets"],'peeling_new_readyforstep2.parset')), peelparset])
		subprocess.call(['cp', "{0}".format(os.path.join(tools["peelingparsets"],'peeling_new_step2.parset')), peel2parset])
	log.info("Determining sources to peel for {0}...".format(SB))
	if peelsources=="0":
		subprocess.call("{0} -i {1} -p {2} -m {3} -v -n {4} -l {5}".format(tools["peelingnew"], SB, peelparset, skymodel, peelnumsources, fluxlimit), shell=True)
	else:
		subprocess.call("{0} -i {1} -p {2} -m {3} -v -n {4} -s {5} -l {6}".format(tools["peelingnew"], SB, peelparset, skymodel, peelnumsources, peelsources, fluxlimit), shell=True)
	newSB=SB+".peeltmp"
	log.info("Peeling {0}...".format(SB))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {4}/logs/{3}_peeling_calibrate.log 2>&1".format(newSB, peelparset, skymodel, logname, obsid), shell=True)
	if not shortpeel:
		subprocess.call("{0} -f -o {1}.skymodel {1}/instrument/ {2} > {4}/logs/{3}_float_solutions.txt 2>&1".format(tools["peelingfloat"], newSB, skymodel, logname, obsid), shell=True)
		subprocess.call("calibrate-stand-alone -f {0} {1} {0}.skymodel > {3}/logs/{2}_peeling_calibrate_step2.log 2>&1".format(newSB, peel2parset, logname, obsid), shell=True)
	#move preepeeled dataset
	subprocess.call('msselect in={0} out={2}/prepeeled_sets/{1} deep=true > {2}/logs/msselect_moveprepeel.log 2>&1'.format(SB, prepeel, obsid), shell=True)
	#rename the peeled dataset
	subprocess.call('msselect in={0} out={1} deep=true > {2}/logs/msselect_movingpeeled.log 2>&1'.format(newSB, SB, obsid), shell=True)
	if os.path.isdir(SB):
		subprocess.call("rm -r {0}.peeltmp".format(SB), shell=True)
	os.remove(p_shiftname)
	os.remove(peelparset)
	if not shortpeel:
		os.remove(peel2parset)
		os.remove("{0}.skymodel".format(newSB))
	subprocess.call("mv calibrate-stand-alone*log logs > logs/peelcalibratelog.log 2>&1", shell=True)

def post_bbs(SB, postcut):
	"""
	Generates a standard NDPPP parset and clips the amplitudes to user specified level.
	"""
	SBsplit=SB.split('/')
	SB_name=SBsplit[-1]
	log.info("Performing post-BBS NDPPP flagging, with cut of {0}, on {1}...".format(postcut, SB_name))
	postbbsfname='ndppp.{0}.postbbs.parset'.format(SB_name)
	ndppp_postbbs=open(postbbsfname,'w')
	ndppp_postbbs.write("msin={0}\n\
msin.datacolumn = CORRECTED_DATA\n\
msout=\n\
msout.datacolumn = CORRECTED_DATA\n\
\n\
steps = [preflag]   # if defined as [] the MS will be copied and NaN/infinite will be  flagged\n\
\n\
preflag.type=preflagger\n\
preflag.corrtype=cross\n\
preflag.amplmax={1}\n\
preflag.baseline=[CS*,RS*,DE*,SE*,UK*,FR*]".format(SB, postcut))
	ndppp_postbbs.close()
	subprocess.call("NDPPP ndppp.{0}.postbbs.parset > {1}/logs/ndppp_postbbs_{0}.txt 2>&1".format(SB_name, SBsplit[0]), shell=True)
	os.remove(postbbsfname)

def convert_newawimager(environ):
	"""
	Returns an environment that utilises the new version of the AWimager for rsm-mainline.
	"""
	environ['LOFARROOT']="/opt/share/lofar-archive/2013-02-11-16-46/LOFAR_r_b0fc3f4"
	environ['PATH']="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/opt/share/soft/pathdirs/bin:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/bin"
	environ['LD_LIBRARY_PATH']="/opt/share/soft/pathdirs/lib:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/lib"
	environ['PYTHONPATH']="/opt/share/soft/pathdirs/python-packages:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/python-packages"
	return environ

def create_mask(beam, mask_size, toimage):
	beamc="SAP00{0}".format(beam)
	mask="parsets/{0}.mask".format(beamc)
	for i in toimage:
		if beamc in i:
			g=i
			break
	if not os.path.isdir(mask):
		log.info("Creating {0} mask...".format(beamc))
		skymodel="parsets/{0}.skymodel".format(beamc)
		subprocess.call('makesourcedb in={0} out={0}.temp format=Name,Type,Ra,Dec,I,Q,U,V,ReferenceFrequency=\\\"60e6\\\",SpectralIndex=\\\"[0.0]\\\",MajorAxis,MinorAxis,Orientation > /dev/null 2>&1'.format(skymodel), shell=True)
		mask_command="awimager ms={0} image={1} operation=empty stokes='I'".format(g, mask)
		mask_command+=mask_size
		subprocess.call(mask_command+" > logs/aw_mask_creation_{0}.log 2>&1".format(beamc), shell=True)
		subprocess.call("{0} {1} {2}.temp > logs/msss_mask.log 2>&1".format(tools["msssmask"], mask, skymodel), shell=True)
		subprocess.call(["rm", "-r", "{0}.temp".format(skymodel)])

def AW_Steps(g, aw_sets, maxb, aw_env, niter, automaticthresh, bandsthreshs_dict, initialiter, uvORm, userthresh, usemask, mos):
	"""
	Performs imaging with AWimager using user supplied settings.
	"""
	c=299792458.
	if g.find("/"):
		logname=g.split("/")[-1]
	else:
		logname=g
	obsid=logname.split("_")[0]
	ft = pt.table(g+'/SPECTRAL_WINDOW', ack=False)
	freq = ft.getcell('REF_FREQUENCY',0)
	wave_len=c/freq
	if uvORm == "M":
		UVmax=maxb/(wave_len*1000.)
		localmaxb=maxb
	else:
		UVmax=maxb
		localmaxb=UVmax*wave_len*1000.
	ft.close()
	log.debug("Frequency = {0} Hz".format(freq))
	log.debug("Wavelength = {0} m".format(wave_len))
	log.debug("UVmax = {0}".format(UVmax))
	beam=int(g.split("SAP")[1][:3])
	beamc="SAP00{0}".format(beam)
	finish_iters=niter
	aw_parset_name="aw_{0}.parset".format(g.split("/")[-1])
	if automaticthresh:
		# finish_iters+=initialiter
		curr_band=g.split("BAND")[1][:2]
		local_parset=open(aw_parset_name, 'w')
		local_parset.write("\nms={0}\n\
image={0}.img\n\
niter={1}\n\
threshold={2}Jy\n\
UVmax={3}\n".format(g, initialiter, 6.*bandsthreshs_dict[curr_band],UVmax))
		if usemask:
			mask="parsets/{0}.mask".format(beamc)
			local_parset.write("mask={0}\n".format(mask))
		for i in aw_sets:
			local_parset.write(i)
		local_parset.close()
		log.info("Imaging {0} with AWimager...".format(g))
		subprocess.call("awimager {0} > {1}/logs/awimager_{2}_initial_log.txt 2>&1".format(aw_parset_name, obsid, logname), env=aw_env, shell=True)
		subprocess.call("image2fits in={0}.img.residual out={0}.img.fits > {1}/logs/image2fits.log 2>&1".format(g, obsid), shell=True)
		try:
			thresh=2.5*(getimgstd("{0}.img.fits".format(g)))
		except:
			log.error("FITS {0}.img.fits could not be found!".format(g))
			return
		os.remove("{0}.img.fits".format(g))
	else:
		thresh=userthresh
	log.info("Cleaning {0} to threshold of {1}...".format(g, thresh))
	local_parset=open(aw_parset_name, 'w')
	local_parset.write("\nms={0}\n\
image={0}.img\n\
niter={1}\n\
threshold={2}Jy\n\
UVmax={3}\n".format(g, finish_iters, thresh, UVmax))
	if usemask:
		local_parset.write("mask={0}\n".format(mask))
	for i in aw_sets:
		local_parset.write(i)
	local_parset.close()
	subprocess.call("awimager {0} > {1}/logs/awimager_{2}_final_log.txt 2>&1".format(aw_parset_name, obsid, logname), env=aw_env, shell=True)
	subprocess.call("image2fits in={0}.img.restored.corr out={0}.img.fits > {1}/logs/image2fits.log 2>&1".format(g, obsid), shell=True)
	if mos:
		subprocess.call("cp -r {0}.img.restored.corr {0}.img_mosaic.restored.corr".format(g), shell=True)
		subprocess.call("cp -r {0}.img0.avgpb {0}.img_mosaic0.avgpb".format(g), shell=True)
	subprocess.call("addImagingInfo {0}.img.restored.corr '' 0 {3} {0} > {1}/logs/addImagingInfo_{2}_log.txt 2>&1".format(g, obsid, logname, localmaxb), shell=True)
	os.remove(aw_parset_name)

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

def average_band_images(snap, beams):
	for b in beams:
		log.info("Averaging {0} SAP00{1}...".format(snap, b))
		subprocess.call("{0} {1}/images/{1}_SAP00{2}_AVG {1}/images/{1}_SAP00{2}_BAND0?.MS.dppp.img.fits > {1}/logs/average_SAP00{2}_log.txt 2>&1".format(tools["average"], snap, b), shell=True)
	
def create_mosaic(snap, band_nums, chosen_environ, pad, avgpbr, ncp):
	for b in band_nums:
		tocorrect=sorted(glob.glob(os.path.join(snap, "images","L*_SAP00?_BAND0{0}.MS.dppp.img_mosaic0.avgpb".format(b))))
		for w in tocorrect:
			wname=w.split("/")[-1]
			if chosen_environ=='rsm-mainline' and pad > 1.0:
				log.info("Correcting {0} mosaic padding...".format(wname))
				avgpb=pt.table("{0}".format(w), ack=False, readonly=False)
				coordstable=avgpb.getkeyword('coords')
				coordstablecopy=coordstable.copy()
				value1=coordstablecopy['direction0']['crpix'][0]
				value2=coordstablecopy['direction0']['crpix'][1]
				value1*=pad
				value2*=pad
				# value1=960.0
				# value2=960.0
				newcrpix=np.array([value1, value2])
				coordstablecopy['direction0']['crpix']=newcrpix
				avgpb.putkeyword('coords', coordstablecopy)
				avgpb.close()
			log.info("Zeroing corners of avgpb {0}...".format(wname))
			subprocess.call("{0} -r {1} {2} > {3}/logs/avgpbz_{4}_log.txt 2>&1".format(tools["mosaicavgpb"], avgpbr, w, snap, wname), shell=True)
		tomosaic=sorted(glob.glob(os.path.join(snap, "{0}_SAP00?_BAND0{1}.MS.dppp".format(snap,b))))
		if not os.path.isdir(os.path.join(snap, "images", "mosaics")):
			os.mkdir(os.path.join(snap, "images", "mosaics"))
		log.info("Creating {0} BAND0{1} Mosaic...".format(snap, b))
		m_list=[i.split("/")[0]+"/images/"+i.split("/")[-1]+".img_mosaic" for i in tomosaic]
		m_name=os.path.join(snap, "images", "mosaics", "{0}_BAND0{1}_mosaic.fits".format(snap, b))
		m_sens_name=os.path.join(snap, "images", "mosaics", "{0}_BAND0{1}_mosaic_sens.fits".format(snap, b))
		if ncp:
			subprocess.call("python {0} -o {1} -N -a avgpbz -s {2} {3} > {4}/logs/mosaic_band0{5}_log.txt 2>&1".format(tools["mosaic"], m_name, m_sens_name, ",".join(m_list), snap, b), shell=True)
		else:
			subprocess.call("python {0} -o {1} -a avgpbz -s {2} {3} > {4}/logs/mosaic_band0{5}_log.txt 2>&1".format(tools["mosaic"], m_name, m_sens_name, ",".join(m_list), snap, b), shell=True)

correct_lofarroot={'/opt/share/lofar-archive/2013-06-20-19-15/LOFAR_r23543_10c8b37':'rsm-mainline', '/opt/share/lofar/2013-09-30-16-27/LOFAR_r26772_1374418':'lofar-sept2013', '/opt/share/lofar/2014-01-22-15-21/LOFAR_r28003_357357b':'lofar-jan2014'}
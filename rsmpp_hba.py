#!/usr/bin/env python

#rsmpp_hba.py

#LOFAR RSM data processing script designed for the Southampton lofar machines.

#A full user guide can be found on google docs here:
# https://docs.google.com/document/d/1aqUxesq4I02i1mKJw_XHjLy0smCbL37uhtBpNf9rs9w

#Written by Adam Stewart, Last Update May 2014

#---Version 2.1.0---

import subprocess, multiprocessing, os, glob, optparse, sys, datetime, string, getpass, time, logging, ConfigParser, base64
from functools import partial
from multiprocessing import Pool
import pyrap.tables as pt
from itertools import izip
import numpy as np
#import stuff for email
import emailslofar as em
vers="2.1.0"	#Current version number

import rsmpp_hbafuncs as rsmhbaf
import rsmppsharedfuncs as rsmshared

mainrootpath=os.path.realpath(__file__)
mainrootpath=mainrootpath.split("/")[:-1]
mainrootpath="/"+os.path.join(*mainrootpath)

#Check environment
curr_env=os.environ

if "LOFARROOT" not in curr_env:
	print "LOFAR Environment not detected!"
	print "Make sure it's initialised by running:"
	print ". /opt/share/lofar/init-lofar.sh (Soton)"
	print ". /pi1storage/soft/lofar/init-lofar.sh (Oxford)"

if curr_env["LOFARROOT"] in rsmshared.correct_lofarroot:
	chosen_environ=rsmshared.correct_lofarroot[curr_env["LOFARROOT"]]
else:
	chosen_environ=curr_env["LOFARROOT"].split("/")[-2]
	
# print "Running on {0} version of lofar software".format(chosen_environ)

config_file="rsmpp_hba.parset"

#Cheat to create extra option 'setup'
if len(sys.argv) > 1:
	if sys.argv[1]=="--setup":
		print "Copying parset file..."
		subprocess.call(["cp", os.path.join(mainrootpath, config_file), "."])
		print "Copying parsets directory..."
		subprocess.call(["cp", "-r", os.path.join(mainrootpath, "parsets"), "parsets"])
		subprocess.call(["cp", os.path.join(mainrootpath, "to_process.py"), "."])
		print "Now ready for pipeline run"
		sys.exit()

#Check for parset file
if not os.path.isfile(config_file):
	# subprocess.call(["cp", os.path.join(mainrootpath, config_file), "."])
	print "The parset file 'rsmpp.parset' could not be found make sure your directory is setup for the pipeline correctly by entering:\n\
rsmpp_hba.py --setup"
	sys.exit()

#Read in the config file
config = ConfigParser.ConfigParser()
config.read(config_file)

#Few date things for naming and user
user=getpass.getuser()
now=datetime.datetime.utcnow()

date_time_start=now.strftime("%d-%b-%Y %H:%M:%S")
newdirname="rsmpp_{0}".format(now.strftime("%H:%M:%S_%d-%b-%Y"))
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Optparse and linking to parameters + checks
#----------------------------------------------------------------------------------------------------------------------------------------------
usage = "usage: python %prog [options]"
description="This script has been written to act as a pipeline for RSM data, which is processed using a HBA MSSS style method. All parsets should be placed in a 'parsets' directory in the \
working area, and the to_process.py script is required which specifies the list of observations or snapshots to process.\n\
For full details on how to run the script, see the user manual here: https://docs.google.com/document/d/1IWtL0Cv-x5Y5I_tut4wY2jq7M1q8DJjkUg3mCWL0r4E"
parser = optparse.OptionParser(usage=usage,version="%prog v{0}".format(vers), description=description)
#define all the options for optparse
group = optparse.OptionGroup(parser, "General Options")
group.add_option("--nice", action="store", type="int", dest="nice", default=config.getint("GENERAL", "nice"), help="Set nice level for processing [default: %default]")
group.add_option("--loglevel", action="store", type="string", dest="loglevel", default=config.get("GENERAL", "loglevel"),help="Use this option to set the print out log level ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'] [default: %default]")
group.add_option("-D", "--lightweight", action="store_true", dest="destroy", default=config.getboolean("GENERAL", "lightweight"),help="Use this option to delete all the output except images, logs and plots [default: %default]")
group.add_option("-n", "--ncores", action="store", type="int", dest="ncores", default=config.getint("GENERAL", "ncores"), help="Specify the number of observations to process simultaneously (i.e. the number of cores to use)[default: %default]")
group.add_option("-o", "--output", action="store", type="string", dest="newdir", default=config.get("GENERAL", "output"),help="Specify name of the directoy that the output will be stored in [default: %default]")
group.add_option("-w", "--overwrite", action="store_true", dest="overwrite", default=config.getboolean("GENERAL", "overwrite"),help="Use this option to overwrite output directory if it already exists [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "LTA Options")
group.add_option("--LTAfetch", action="store_true", dest="lta", default=config.getint("LTA", "LTAfetch"), help="Turn on or off LTA data fetching [default: %default]")
group.add_option("--method", action="store", type="choice", choices=["html","srm"], dest="ltameth", default=config.get("LTA", "method"),help="Select to use 'html' or 'srm' for data transfer [default: %default]")
group.add_option("--htmlfile", action="store", type="string", dest="htmlfile", default=config.get("LTA", "htmlfile"),help="LTA html.txt file with wget addresses [default: %default]")
group.add_option("--n_simult_dwnlds", action="store", type="int", dest="ltacores", default=config.getint("LTA", "n_simult_dwnlds"), help="Specify the number of simultaneous downloads [default: %default]")
group.add_option("--missing_attempts", action="store", type="int", dest="missattempts", default=config.getboolean("LTA", "missing_attempts"),help="How many attempts will be made to retrive failed downloads (i.e. missing files from the html file) [default: %default]")
group.add_option("--delay", action="store", type="int", dest="ltadelay", default=config.getint("LTA", "delay"), help="Time in seconds between each missing file attempt [default: %default]")
group.add_option("--savedir", action="store", type="string", dest="ltadir", default=config.get("LTA", "savedir"),help="Directory to save data from LTA [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Data Options")
group.add_option("--obsids", action="store", type="string", dest="obsids", default=config.get("DATA","obsids"), help="Use this to bypass using to_process.py, manually list the ObsIds you want to run in the format\
'L81111,L81112,L81113,L81114,...' (No spaces!) [default: %default]")
group.add_option("-d", "--datadir", action="store", type="string", dest="datadir", default=config.get("DATA", "datadir"),help="Specify name of the directoy where the data is held (in obs subdirectories) [default: %default]")
group.add_option("-B", "--bandsno", action="store", type="int", dest="bandsno", default=config.getint("DATA", "bandsno"),help="Specify how many bands there are. [default: %default]")
group.add_option("-S", "--subsinbands", action="store", type="int", dest="subsinbands", default=config.getint("DATA", "subsinbands"),help="Specify how sub bands are in a band. [default: %default]")
group.add_option("-b", "--beams", action="store", type="string", dest="beams", default=config.get("DATA", "beams"), help="Use this option to select which beams to process in the format of a list of beams with no spaces \
separated by commas eg. 0,1,2 [default: %default]")
group.add_option("-j", "--target_oddeven", action="store", type="string", dest="target_oddeven", default=config.get("DATA", "target_oddeven"),help="Specify whether the targets are the odd numbered observations or the even [default: %default]")
group.add_option("--precalibrated", action="store_true", dest="precalib", default=config.getboolean("DATA", "precalibrated"),help="Select this option if the data has been precalibrated by ASTRON [default: %default]")
group.add_option("--precalibratedloc", action="store", type="choice", choices=['DATA', 'CORRECTED_DATA',], dest="precalibloc", default=config.get("DATA", "precalibratedloc"), help="Define whether the calibrated data is located in the DATA or CORRECTED_DATA column [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Processing Options")
group.add_option("-f", "--flag", action="store_true", dest="autoflag", default=config.getboolean("PROCESSING", "autoflag"),help="Use this option to use autoflagging in processing [default: %default]")
group.add_option("-t", "--postcut", action="store", type="int", dest="postcut", default=config.getint("PROCESSING", "postcut"),help="Use this option to enable post-bbs flagging, specifying the cut level [default: %default]")
group.add_option("-P", "--PHASEONLY", action="store_true", dest="PHASEONLY", default=config.getboolean("PROCESSING", "PHASEONLY"),help="Choose just to perform only a phase only calibration on an already EXISTING rsmpp output [default: %default]")
group.add_option("--phaseonly_name", action="store", type="string", dest="phase_name", default=config.get("PROCESSING", "phaseonly_name"),help="Specifcy the name of the output directory of the phase only mode [default: %default]")
group.add_option("--phaseonly_col", action="store", type="choice", dest="phase_col", choices=['DATA', 'CORRECTED_DATA',], default=config.get("PROCESSING", "phaseonly_col"),help="Choose which column to pull the data from ('DATA' or 'CORRECTED_DATA') [default: %default]")
group.add_option("--phaseonly_bands", action="store", type="string", dest="phase_pattern", default=config.get("PROCESSING", "phaseonly_bands"),help="Specifcy the glob pattern to select the bands to perform the phase-cal on (eg. '??' selects L*/L*BAND??_*.dppp) [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Parset Options:")
group.add_option("-k", "--ndppp", action="store", type="string", dest="ndppp", default=config.get("PARSETS", "ndppp"),help="Specify the template initial NDPPP file to use [default: %default]")
group.add_option("-a", "--calparset", action="store", type="string", dest="calparset", default=config.get("PARSETS", "calparset"),help="Specify bbs parset to use on calibrator calibration [default: %default]")
group.add_option("-g", "--corparset", action="store", type="string", dest="corparset", default=config.get("PARSETS", "corparset"),help="Specify bbs parset to use on gain transfer to target [default: %default]")
group.add_option("-z", "--phaseparset", action="store", type="string", dest="phaseparset", default=config.get("PARSETS", "phaseparset"),help="Specify bbs parset to use on phase only calibration of target [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Skymodel Options:")
group.add_option("-e", "--calmodel", action="store", type="string", dest="calmodel", default="AUTO",help="Specify a calibrator skymodel. By default the calibrator will be \
detected and the respective model will be automatically fetched [default: %default]")
group.add_option("-s", "--targetmodel", action="store", type="string", dest="skymodel", default=config.get("SKYMODELS", "targetmodel"),help="Specify a particular field skymodel to use for the phase only calibration, by default the skymodels will be\
automatically generated.[default: %default]")
group.add_option("-r", "--targetradius", action="store", type="float", dest="skyradius", default=config.getfloat("SKYMODELS", "targetradius"), help="Radius of automatically generated field model [default: %default]")
group.add_option("-y", "--dummymodel", action="store", type="string", dest="dummymodel", default=config.get("SKYMODELS", "dummymodel"),help="Specify dummy model for use in applying gains [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Peeling Options:")
group.add_option("-p", "--peeling", action="store_true", dest="peeling", default=config.getboolean("PEELING", "peeling"),help="Use this option to enable peeling [default: %default]")
group.add_option("-q", "--peelnumsources", action="store", type="int", dest="peelnumsources", default=config.getint("PEELING", "peelnumsources"),help="Use this option to specify how many sources to peel [default: %default]")
group.add_option("-l", "--peelfluxlimit", action="store", type="float", dest="peelfluxlimit", default=config.getfloat("PEELING", "peelfluxlimit"),help="Specify the minimum flux to consider a source for peeling (in Jy) [default: %default]")
group.add_option("-v", "--peelingshort", action="store_true", dest="peelingshort", default=config.getboolean("PEELING", "peelingshort"),help="Use this option to skip the last section of the peeling procedure and NOT add back in the peeled sources [default: %default]")
group.add_option("-c", "--peelsources", action="store", type="string", dest="peelsources", default=config.get("PEELING", "peelsources"),help="Use this option to specify which sources to peel instead of the code taking the X brightest sources. Enter in the format\
 source1,source2,source3,.... [default: None]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Imaging Options:")
group.add_option("-i", "--imaging", action="store_true", dest="imaging", default=config.getboolean("IMAGING", "imaging"),help="Set whether you wish the data to be imaged. [default: %default]")
group.add_option("-A", "--automaticthresh", action="store_true", dest="automaticthresh", default=config.getboolean("IMAGING", "automaticthresh"),help="Switch on automatic threshold method of cleaning [default: %default]")
group.add_option("-I", "--initialiter", action="store", type="int", dest="initialiter", default=config.getint("IMAGING", "initialiter"),help="Define how many cleaning iterations should be performed in order to estimate the threshold [default: %default]")
group.add_option("-R", "--bandrms", action="store", type="string", dest="bandrms", default=config.get("IMAGING", "bandrms"),help="Define the prior level of expected band RMS for use in automatic cleaning, enter as '0.34,0.23,..' no spaces, in units of Jy [default: %default]")
group.add_option("-U", "--maxbunit", action="store", type="choice", dest="maxbunit", choices=['UV', 'm',], default=config.get("IMAGING", "maxbunit"),help="Choose which method to limit the baselines, enter 'UV' for UVmax (in klambda) or 'm' for physical length (in metres) [default: %default]")
group.add_option("-L", "--maxbaseline", action="store", type="int", dest="maxbaseline", default=config.getfloat("IMAGING", "maxbaseline"),help="Enter the maximum baseline to image out to, making sure it corresponds to the unit options [default: %default]")
group.add_option("-m", "--mask", action="store_true", dest="mask", default=config.getboolean("IMAGING", "mask"), help="Use option to NOT use a mask when cleaning [default: %default]")
group.add_option("-M", "--mosaic", action="store_true", dest="mosaic", default=config.getboolean("IMAGING", "mosaic"),help="Use option to produce snapshot, band, mosaics after imaging [default: %default]")
group.add_option("--avgpbradius", action="store", type="float", dest="avgpbrad", default=config.get("IMAGING", "avgpbrad"),help="Choose radius for which avgpbz.py trims the primary beam when mosaicing [default: %default]")
parser.add_option_group(group)
(options, args) = parser.parse_args()

#check to see if email is possible, if user is a known address or not
try:
	emacc=em.load_account_settings_from_file(os.path.join(mainrootpath, ".email_acc"))
	known_users=em.known_users
	user_address=base64.b64decode(known_users[user])
	mail=True
	#turn mail off if not configured
	if emacc['email_account_password']=="contactme":
		mail=False
except:
	mail=False

#Set nice level
os.nice(options.nice)
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Set up logging
#----------------------------------------------------------------------------------------------------------------------------------------------

#Check log level selected, could be a choice in optparse but this allows for better error message to user
allowedlevels=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
loglevel=options.loglevel.upper()
if loglevel not in allowedlevels:
	print "Logging level {0} not recongised\n\
Must be one of {1}".format(loglevel, allowedlevels)
	sys.exit()

#Fetches the numeric level of the defined level
numeric_level = getattr(logging, loglevel, None)

#Check if it will run in phase only mode before logging is started
phaseO=options.PHASEONLY
if phaseO:
	phase_name=options.phase_name
	phase_col=options.phase_col
	phase_pattern=options.phase_pattern

#Setup logging
log=logging.getLogger("rsm")
log.setLevel(logging.DEBUG)
logformat=logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
term=logging.StreamHandler()
term.setLevel(numeric_level)
term.setFormatter(logformat)
log.addHandler(term)

#set logging appropriately 
if phaseO:
	textlog=logging.FileHandler('rsmpp_phaseonly.log', mode='w')
else:
	textlog=logging.FileHandler('rsmpp_hba.log', mode='w')
textlog.setLevel(logging.DEBUG)
textlog.setFormatter(logformat)
log.addHandler(textlog)

log.info("Run started at {0} UTC".format(date_time_start))
log.info("rsmpp.py Version {0}".format(vers))
log.info("Running on {0} lofar software".format(chosen_environ))
log.info("User: {0} - email set to {1}".format(user, mail))

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Assignment to variables
#----------------------------------------------------------------------------------------------------------------------------------------------

#Set options to variables just to make life a bit easier
precal=options.precalib	#precalibrated mode
precalloc=options.precalibloc	#precalibrated data location
autoflag=options.autoflag	#autoflagging on or off
imaging_set=options.imaging	#if imaging is to be performed
automaticthresh=options.automaticthresh	#if the automated imaging threshold strategy should be used
initialiters=options.initialiter #Initial imaging iterations
bandthreshs=options.bandrms	#the band thresholds
maxbunit=options.maxbunit.upper()	#unit of maximum baseline lenght (UV or M)
maxb=options.maxbaseline	#The maximum baselines to use (keeping in mind the unit)
mask=options.mask	#no mask
mosaic=options.mosaic	#mosaic on off
avpbrad=options.avgpbrad	#avgpbz radius
ndppp_parset=options.ndppp #the ndppp parset name	
n=options.ncores	#number of threads to use
newdirname=options.newdir	#name of output directory
peeling=options.peeling	#peeling on off
peelnumsources=options.peelnumsources	#number of sources to peel
peelfluxlimit=options.peelfluxlimit	#flux limit of peeling sources
shortpeel=options.peelingshort	#do not add the sources back in on off
peelsources_todo=options.peelsources	#specify individual sources to peel
postcut=options.postcut	#level of post bbs NDPPP to flag down to
overwrite=options.overwrite	#to overwrite output directory if already exists
toflag=""	#baselines to flag variable, starts off blank
calparset=options.calparset	#calibrator bbs parset
data_dir=options.datadir	#directory where data to process is located
calmodel=options.calmodel	#calibrator model
correctparset=options.corparset	#correct parset to transfer solutions
target_oddeven=options.target_oddeven.lower()	#if the targets are the odd of even numbered ids
phaseparset=options.phaseparset	#phase only parset
dummy=options.dummymodel	#dummy model for transfer
skymodel=options.skymodel	#skymodel used in phase calibration
root_dir=os.getcwd()	#where the script is run from
destroy=options.destroy	#lightweight mode on or off
bandsno=options.bandsno	#number of bands there should be
subsinbands=options.subsinbands	#number of sub bands to combine to make a band
toprocess=options.obsids	#toprocess variable - what ID's to run
lta=options.lta	#fetch data from lta on or off
html=options.htmlfile	#lta html file
missattempts=options.missattempts	#lta missing attempts
ltadelay=options.delay	#delay between attempts
ltadir=options.ltadir	#save data to directory
ltacores=options.ltacores #number of downloads
ltameth=options.ltameth #htmlorsrc

#Now gather the ids that will be run, either from a file or if not a text input.
if toprocess!="to_process.py":
	if "," in toprocess:
		toprocess_list=sorted(toprocess.split(","))
	elif "-" in toprocess:
		tempsplit=sorted(toprocess.split("-"))
		toprocess_list=["L{0}".format(i) for i in range(int(tempsplit[0].split("L")[-1]),int(tempsplit[1].split("L")[-1])+1)]
#get the beams to run
beams=[int(i) for i in options.beams.split(',')]
nchans=0

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Checks & True False Definitions
#----------------------------------------------------------------------------------------------------------------------------------------------
#Below are all just various file and setting checks etc to make sure everything is present and correct or within limits. Some options are not simply
#True or False so flags also have to be set for such options.

log.info("Performing initial checks and assigning settings...")
#Checks the actual parset folder
if os.path.isdir(os.path.join(root_dir, "parsets")) == False:
	log.critical("Parsets directory cannot be found.\n\
Please make sure all parsets are located in a directory named 'parsets'.\n\
Script now exiting...")
	sys.exit()

#Checks number of threads is reasonable
if 0 > n or n > multiprocessing.cpu_count():
	log.critical("Number of cores must be between 1 - {0}\n\
Script now exiting...".format(multiprocessing.cpu_count()))
	sys.exit()

#Whether flagging is to be used
if toflag !="" or autoflag ==True:	#If user selects stations to flag then flag also needs to be set to true.
	flag=True
else:
	flag=False

#Imaging Check inc parsets are present
if imaging_set:
	bandsthreshs_dict={}
	if os.path.isfile("parsets/aw.parset") == False:
		log.critical("Cannot find imaging parset file 'aw.parset' in the 'parsets' directory, please check it is present\n\
Script now exiting...")
		sys.exit()
	#if automatic method we then need to check we have the correct number of band thresholds
	if automaticthresh:
		tempbandsthresh=bandthreshs.split(",")
		if len(tempbandsthresh) < bandsno:
			log.critical("Number of thresholds given is less than the number of bands")
			sys.exit()
		else:
			for i in range(0, len(tempbandsthresh)):
				bandsthreshs_dict["{0:02d}".format(i)]=float(tempbandsthresh[i])
	#check baseline selections
		

#Checks if post bbs is to be used.
if postcut !=0:
	postbbs=True
else:
	postbbs=False

#Check presence of to_process.py if needed
if toprocess=="to_process.py":
	if os.path.isfile(toprocess)==False:
		log.critical("Cannot locate 'to_process.py', please check file is present\nScript now exiting...")
		sys.exit()

#Check skymodel creation choice or file
if skymodel=="AUTO":
	create_sky=True
else:
	if not os.path.isfile(skymodel):
		log.error("Cannot locate {0}, please check your skymodel file is present\n\
If you would like to automatically generate a skymodel file do not use the -s option.\nScript now exiting...".format(skymodel))
		sys.exit()
	else:
		create_sky=False
		
if calmodel=="AUTO":
	create_cal=True
else:
	if not os.path.isfile(calmodel):
		log.error("Cannot locate {0}, please check your calmodel file is present\n\
If you would like to automatically fetch the calibrator skymodel set the calmodel option to 'AUTO'.\nScript now exiting...".format(skymodel))
		sys.exit()
	else:
		create_cal=False	

#LTA checks
if lta:
    if ltameth=="html":
    	userhome=os.path.expanduser("~")
    	wgetfile=os.path.join(userhome, ".wgetrc")
    	if not os.path.isfile(wgetfile):
    		log.critical("No '.wgetrc' file detected in home directory - LTA wget will fail.")
    		log.critical("Please set up and try again.")
    		sys.exit()
	if not os.path.isfile(html):
		log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(ndppp_parset))
		sys.exit()	
	if ltadir!=data_dir:
		data_dir=ltadir
	if not os.path.isdir(data_dir):
		try:
			os.makedirs(data_dir)
		except:
			log.critical("The destination directory for the LTA transfer doesn't seem to exist and cannot be created")
			log.critical("Script now exiting...")
			sys.exit()
	
log.info("Checking required parsets...")
#NDPPP parset
if not os.path.isfile(ndppp_parset):
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(ndppp_parset))
	sys.exit()
#Check data dir
if not os.path.isdir(data_dir):
	log.critical("Data Directory \"{0}\" doesn't seem to exist..., please check it has been set correctly.\n\
Script now exiting...".format(data_dir))
	sys.exit()
#Check the phase only parset
if not os.path.isfile(phaseparset):
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(phaseparset))
	sys.exit()
#Checks presence of the parset files
if not os.path.isfile(calparset):
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(calparset))
	sys.exit()
if not os.path.isfile(correctparset):
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(correctparset))
	sys.exit()
if not os.path.isfile(dummy):
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(dummy))
	sys.exit()

#----------------------------------------------------------------------------------------------------------------------------------------------
#																		PHASE ONLY STAGE
#----------------------------------------------------------------------------------------------------------------------------------------------

if phaseO:
	log.info("Running PHASE ONLY Calibration")
	try:
		if not os.path.isdir(newdirname):
			log.critical("{0} output directory cannot be found!")
			sys.exit()
		else:
			os.chdir(newdirname)
			working_dir=os.getcwd()
			#Get new parset and skymodel if not present
			if not os.path.isfile(phaseparset):
				checkforphase=os.path.join("..",phaseparset)
				if os.path.isfile(checkforphase):
					subprocess.call("cp {0} parsets/".format(checkforphase), shell=True)
				else:
					log.critical("Cannot find phase parset in results or main parsets directory!")
					sys.exit()
			if not create_sky:
				if not os.path.isfile(skymodel):
					checkformodel=os.path.join("..",skymodel)
					if os.path.isfile(checkformodel):
						subprocess.call("cp {0} parsets/".format(checkformodel), shell=True)
					else:
						log.critical("Cannot find sky model in results or main  parsets directory!")
						sys.exit()
			ids_present=sorted(glob.glob("L?????*"))
			for id in ids_present:
				newoutput=os.path.join(id, phase_name)
				if os.path.isdir(newoutput):
					if overwrite:
						log.info("Removing old phase only output...")
						subprocess.call("rm -rf {0}".format(newoutput), shell=True)
					else:
						log.critical("{0} already exists! Use overwrite option or change output name.".format(newoutput))
						sys.exit()
				os.mkdir(newoutput)
		tophase=sorted(glob.glob("L*/L*BAND{0}*.dppp".format(phase_pattern)))
		workers=Pool(processes=n)
		standalone_phase_multi=partial(rsmshared.standalone_phase, phaseparset=phaseparset, flag=flag, toflag=toflag, autoflag=autoflag, create_sky=create_sky, skymodel=skymodel, phaseoutput=phase_name, phasecolumn=phase_col)
		workers.map(standalone_phase_multi, tophase)
		log.info("All finished successfully")
		end=datetime.datetime.utcnow()
		date_time_end=end.strftime("%d-%b-%Y %H:%M:%S")
		tdelta=end-now
		if mail==True:
			em.send_email(emacc,user_address,"rsmpp Job PHASE ONLY Completed","{0},\n\nYour job {1} has been completed - finished at {2} UTC with a runtime of {3}".format(user,newdirname, date_time_end, tdelta))
		os.chdir("..")
		subprocess.call("rm emailslofar.py* quick_keys.py*", shell=True)
		log.info("Run finished at {0} UTC with a runtime of {1}".format(date_time_end, str(tdelta)))
		subprocess.call(["cp", "rsmpp_phaseonly.log", "{0}/rsmpp_phaseonly_{0}.log".format(newdirname)])
	except Exception, e:
		log.exception(e)
		if mail==True:
			em.send_email(emacc,user_address,"rsmpp Job Error","{0},\n\nYour phase only job {1} crashed with the following error:\n\n{2}".format(user,newdirname,e))
			em.send_email(emacc,"adam.stewart@soton.ac.uk","rsmpp Job Error","{0}'s job '{1}' just crashed with the following error:\n\n{2}\n\nTime of crash: {3}".format(user,newdirname,e))
	
	
else:

	#----------------------------------------------------------------------------------------------------------------------------------------------
	#																Other Pre-Run Checks & Directory Change
	#----------------------------------------------------------------------------------------------------------------------------------------------

	# Checks that the output directory is not already present, overwrites if -w is used	
	if os.path.isdir(newdirname) == True:
		if overwrite==True:
			log.info("Removing previous results directory...")
			subprocess.call("rm -rf {0}".format(newdirname), shell=True)
		else:
			log.critical("Directory \"{0}\" already exists and overwrite option not used, run again with '-w' option to overwrite directory or rename/move old results file\n\
	Script now exiting...".format(newdirname))
			sys.exit()
	
	# Makes the new directory and moves to it
	os.mkdir(newdirname)
	os.chdir(newdirname)
	working_dir=os.getcwd()

	# Copies over all relevant files needed
	subprocess.call(["cp","-r","../parsets", "."])
	if toprocess=="to_process.py":
		subprocess.call(["cp","-r","../to_process.py", "."])
	if not os.path.isdir('logs'):
		os.mkdir('logs')
	#copy over parset file used
	if lta:
		subprocess.call(["cp","-r","{0}".format(os.path.join("..", html)), "."])
	subprocess.call(["cp",os.path.join("..", config_file), config_file+"_used"])
	
	#----------------------------------------------------------------------------------------------------------------------------------------------
	#																			LTA Fetch
	#----------------------------------------------------------------------------------------------------------------------------------------------

	if lta:
		log.info("LTA data fetch starting...")
		#Set up LTA specific workers for downloading
		lta_workers=Pool(processes=ltacores)
		#Fetch the html file to be used
		subprocess.call(["cp","-r",html, data_dir])
		#Switch to data_dir, read in html file and start downloading
		os.chdir(data_dir)
		html_temp=open(file, 'r')
		initfetch=[htmlline.rstrip('\n') for htmlline in html_temp]
		html_temp.close()
		log.info("Fetching Files...")
        if ltameth=="html":
    		lta_workers.map(rsmshared.fetch, initfetch)
        else:
            lta_workers.map(rsmshared.fetchgrid, initfetch)
		log.info("Initial fetch complete!")
		#Start the checking for missing files
		log.info("Checking for missing files...")
		for attempt in range(missattempts):
			log.info("----------------------------------------------------------------------------------------")
			log.info("Running Missing File Check {0} of {1}".format(attempt+1, missattempts))
			#Files downloaded should match those in the html file with a few changes
            if ltameth=="html":
    			tofetch=[k for k in initfetch if not os.path.isfile('SRMFifoGet'+k.split('SRMFifoGet')[-1].replace('/', '%2F'))]
            else:
                tofetch=[k for k in initfetch if not os.path.isfile(k.split('file:///')[-1])]
			if len(tofetch) < 1:
				log.info("0 files remain to fetch")
				#if no more missing then break the for loop
				break
			else:
				log.warning("{0} files remain to fetch:".format(len(tofetch)))
				log.info("----------------------------------------")
				#Print out missing files and proceed to attempt to fetch with delay
				for ltafile in tofetch:
					log.info(ltafile.split("/")[-1])
				log.info("----------------------------------------")
				log.info("Waiting {0} seconds before attempting to fetch missing files...")
				time.sleep(ltadelay)
                if ltameth=="html":
    				lta_workers.map(rsmshared.fetch, tofetch)
                else:
                    lta_workers.map(rsmshared.fetchgrid, tofetch)
				
		log.info("LTA fetch complete!")
		#Need to prepare data for pipeline: untar -> rename -> organise into dirs
		log.info("Preparing data for pipeline use...")
		ltaoutput=sorted(glob.glob("*.tar"))
		log.info("Unpacking data...")
		for tar in ltaoutput:
			#Untar files one at a time as doing multiple really hits disc writing speed
			rsmshared.untar(tar)
		ltaoutput2=sorted(glob.glob("*.MS"))
		if len(ltaoutput2)<1:
			#Stop the pipeline if something has gone wrong and no .MS files are present
			log.critical("No data files detected after unpacking! Did the download work?")
			sys.exit()
		else:
			log.info("Renaming LTA output...")
			lta_workers=Pool(processes=n)
			lta_workers.map(rsmshared.rename1, ltaoutput2)
			log.info("Organising files...")
			ltaoutput3=sorted(glob.glob("*.dppp"))
			#Obtain a list of unique IDs
			ltaobsids=[ltams.split("_")[0] for ltams in ltaoutput3]
			uniq_ltaobsids=sorted(list(set(ltaobsids)))
			for lta_id in uniq_ltaobsids:
				#Check if directory already exists
				if os.path.isdir(lta_id):
					log.critical("Obs ID directory already exists in data directory - will not overwrite or move files.".)
					log.critical("Please check and organise the downloaded data - rsmpp-rename.py can help with this.".)
					log.critical("Once done the pipeline can be re-ran with LTA mode off, just point to the data directory.".)
					sys.exit()
				else:	
					os.mkdir(lta_id)
			lta_workers.map(rsmshared.organise, ltaoutput3)
			#The IDs should match those defined in to_process
			obsids_toremove=[]
			for obsid in to_process:
				if obsid not in uniq_ltaobsids:
					log.warning("Requested ObsID to process {0} doesn't seem to have been downloaded from the LTA - removed from processing list.".format(obsid))
					obsids_toremove.append(obsid)
			#remove obs ids outside of loop otherwise problems occur
			for obsdel in obsids_toremove:
				to_process.remove(obsdel)
			#Perhaps user forgot to change obs ids in to process - check for this and switch to the ids downloaded if necessary
			if len(to_process) < 2:
				log.warning("The to_process obs ids list contains less than the minimum number required.")
				log.warning("Switching to process the data downloaded from the LTA.")
				to_process=uniq_ltaobsids
					
			log.info("Data ready for pipeline use!")
			log.info("Removing LTA tar files...")
			lta_workers.map(rsmshared.deletefile, ltaoutput)
			log.info("Done!")
			log.info("All LTA steps completed.")
		
		#Change back to output directory
		os.chdir(working_dir)
	
	
	#----------------------------------------------------------------------------------------------------------------------------------------------
	#																Load in User List and Check Data Presence
	#----------------------------------------------------------------------------------------------------------------------------------------------
	try:
		#Gets the to_process list and assigns it to target_obs
		log.info("Collecting observations to process...")

		if toprocess=="to_process.py":
			from to_process import to_process
			target_obs=to_process
			target_obs.sort()
		else:
			target_obs=toprocess_list

		#This splits up the sets to process into targets and calibrators.
		odd=[]
		even=[]
		firstid=target_obs[0]
		if int(firstid[-5:])%2 == 0:
			firstid_oe="even"
		else:
			firstid_oe="odd"

		for obs in target_obs:
			if int(obs[-5:])%2 == 0:
				even.append(obs)
			else:
				odd.append(obs)
		
		if target_oddeven=="even":
			target_obs=even
			calib_obs=odd
		else:
			target_obs=odd
			calib_obs=even

		#The following passage just checks that all the data is present where it should be.
		log.info("Observations to be processed:")
		for i in target_obs:
			log.info(i)
			if os.path.isdir(os.path.join(data_dir,i))==False:
				log.critical("Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir))
				sys.exit()
			if not os.path.isdir(i):
				subprocess.call("mkdir -p {0}/logs {0}/flagging {0}/datasets".format(i), shell=True)
		log.info("Calibrators to be processed:")
		for i in calib_obs:
			log.info(i)
			if os.path.isdir(os.path.join(data_dir,i))==False:
				log.critical("Calibrator Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir))
				sys.exit()
			if not os.path.isdir(i):
				subprocess.call("mkdir -p {0}/plots {0}/logs".format(i), shell=True)


		#----------------------------------------------------------------------------------------------------------------------------------------------
		#																Search for Missing Sub bands
		#----------------------------------------------------------------------------------------------------------------------------------------------

		#Get ready for reporting any missing sub bands.
		missing_count=0
		g=open("missing_subbands.txt", 'w')

		#RSM searching, has to check for all snapshots, and calibrators next block of code checks for missing sub bands and organises bands
		targets=rsmshared.Ddict(dict)	#Dictionary of dictionaries to store target observations
		targets_corrupt={}
		calibs={}
		missing_calibrators={}
		corrupt_calibrators={}
		missing_calib_count=0
		rsm_bands={}	#Store the measurement sets in terms of bands
		rsm_bands_lens={}	#Store the length of each band (needed as some might be missing)
		diff=(bandsno*subsinbands)
		# diff=34
		rsm_band_numbers=range(bandsno)
		# rsm_band_numbers=range(3)
		nchans=0

		log.info("Collecting and checking sub bands of observations..")
		log.info("All measurement sets will be checked for any corruption.")
		time.sleep(2)
		for i,j in izip(target_obs, calib_obs):
			missing_calibrators[i]=[]
			corrupt_calibrators[i]=[]
			targets_corrupt[i]=[]
			log.info("Checking Calibrator {0}...".format(j))
			calibglob=os.path.join(data_dir,j,'*.MS.dppp')
			calibs[j]=sorted(glob.glob(calibglob))
			log.debug(calibs[j])
			if len(calibs[j])<1:
				log.critical("Cannot find any measurement sets in directory {0} !".format(os.path.join(data_dir,j)))
				sys.exit()
			calibs_first=0		#Should always start on 0
			calibs_last=int(calibs[j][-1].split('SB')[1][:3])	#Last one present
			calib_range=range(calibs_first, calibs_last+1)		#Range of present (if last one is missing then this will be realised when looking at targets) 
			present_calibs=[]
			for c in calibs[j]:
				calib_name=c.split("/")[-1]
				#Check for corrupt datasets
				try:
					test=pt.table(c,ack=False)
				except:
					log.warning("Calibrator {0} is corrupt!".format(calib_name))
					time.sleep(1)
					corrupt_calibrators[i].append(c)
				else:
					test.close()
					SB=int(c.split('SB')[1][:3])				#Makes a list of all the Calib sub bands present
					present_calibs.append(SB)
			for s in calib_range:
				if s not in present_calibs:
					missing_calibrators[i].append(s)		#Checks which ones are missing and records them
					g.write("SB{0} calibrator missing in observation {1}\n".format('%03d' % s, j))
					missing_count+=1
			for b in beams:
				#This now uses a function to check all the targets, now missing what calibs are missing - which without nothing can be done
				localmiss=rsmhbaf.check_targets(i, b, targets, targets_corrupt, rsm_bands, rsm_band_numbers, rsm_bands_lens, missing_calibrators, data_dir, diff, g, subsinbands)
				missing_count+=localmiss
				#covers run if user accidentally adds a beam which is missing or doesn't exist.
			log.info("{0} and {1} checks done!".format(i,j))

		g.close()

		#Give feedback as to whether any are missing or not.
		if missing_count>0:
			log.warning("Some sub bands appear to be missing - see generated file 'missing_subbands.txt' for details")
		else:
			#Just remove the file if none are missing
			os.remove("missing_subbands.txt")	

		#----------------------------------------------------------------------------------------------------------------------------------------------
		#																Main Run
		#----------------------------------------------------------------------------------------------------------------------------------------------
		#Create multiprocessing Pool
		worker_pool = Pool(processes=n)

		#Reads in NDPPP parset file ready for use
		n_temp=open(ndppp_parset, 'r')
		ndppp_base=n_temp.readlines()
		n_temp.close()
		# 
		# Following is the main run -> create models -> Initial NDPPP -> Calibrate -> (Peeling) -> (Post bbs) -> concatenate
		# 
		#Detect which calibrator (CURRENTLY DEPENDS ALOT ON THE MS TABLE BEING CONSISTANT and CALIBRATOR BEING SAME THROUGHOUT WHOLE OBS and NAMED AS JUST CALIBRATOR)
		#Needs a fail safe of coordinates detection idealy
		if not precal:
			if create_cal:
				log.info("Detecting calibrator and obtaining skymodel...")
				calib_ms=pt.table(calibs[calibs.keys()[0]][0]+"/OBSERVATION", ack=False)
				calib_name=calib_ms.col("LOFAR_TARGET")[0][0].replace(" ", "")
				log.info("Calibrator Detected: {0}".format(calib_name))
				calmodel="{0}.skymodel".format(calib_name)
				if not os.path.isfile(os.path.join(mainrootpath, "skymodels", calmodel)):	#MOVE THESE!
					log.critical("Could not find calibrator skymodel...")
					if mail==True:
						em.send_email(emacc,user_address,"rsmpp Job Error","{0},\n\nYour job {1} has encountered an error - calibrator skymodel could not be found for Calibrator {2}".format(user,newdirname, calib_name))
					sys.exit()
				subprocess.call(["cp", os.path.join(mainrootpath, "skymodels", calmodel), "parsets/"])
				calmodel=os.path.join("parsets", calmodel)
				calib_ms.close()
		
			# Builds parmdb files as these are used over and over
			log.info("Building calibrator sourcedb...")
			if os.path.isdir('sky.calibrator'):
				subprocess.call("rm -rf sky.calibrator", shell=True)
			subprocess.call("makesourcedb in={0} out=sky.calibrator format=\'<\' > logs/skysourcedb.log 2>&1".format(calmodel), shell=True)

		log.info("Building dummy sourcedb...")
		if os.path.isdir('sky.dummy'):
			subprocess.call("rm -rf sky.dummy", shell=True)
		subprocess.call("makesourcedb in={0} out=sky.dummy format=\'<\'  > logs/dummysourcedb.log 2>&1".format(dummy), shell=True)

		#Creates the sky model for each pointing using script that creates sky model from measurement set.
		if create_sky:
			log.info("Creating skymodels for each beam...")
			for b in beams:
				beamc="SAP00{0}".format(b)
				skymodel="parsets/{0}.skymodel".format(beamc)
				rsmshared.create_model(targets[targets.keys()[0]][beamc][0], skymodel, options.skyradius)
				#Check it ran ok
				if not os.path.isfile(skymodel):
					log.critical("Skymodel {0} failed to be created, gsm.py may be broken, cannot continue".format(skymodel))
					raise Exception("Skymodel {0} failed to be created".format(skymodel))
				if imaging_set:
					subprocess.call("makesourcedb in={0} out={1} format=\'<\' > logs/skysourcedb_{2}.log 2>&1".format(skymodel, skymodel.replace(".skymodel", ".sky"), beamc), shell=True)

		#Now working through the steps starting with NDPPP (see rsmppfuncs.py for functions)
		postcorrupt=0
		for i,j in izip(sorted(targets.keys()), sorted(calibs)):
			log.info("Starting Initial NDPPP for {0} and {1}...".format(i, j))
			current_obs=i
			#Nearly all functions are used with partial such that they can be passed to .map
			NDPPP_Initial_Multi=partial(rsmshared.NDPPP_Initial, wk_dir=working_dir, ndppp_base=ndppp_base, prec=precal, precloc=precalloc)
			if __name__ == '__main__':
				if not precal:
					worker_pool.map(NDPPP_Initial_Multi, calibs[j])
					calibs[j] = sorted(glob.glob(os.path.join(j,"*.tmp")))
					if not os.path.isfile("post_ndppp_corrupt_report.txt"):
						corrupt_report=open("post_ndppp_corrupt_report.txt", 'w')
						corrupt_report.close()
					log.info("Checking for bad calibrators in {0}...".format(j))
					checkresults=worker_pool.map(rsmshared.check_dataset, calibs[j])
					corrupt_report=open("post_ndppp_corrupt_report.txt", "a")
					for p in checkresults:
						if p!=True:
							calibs[j].remove(p)
							SB_no=int(p.split('SB')[1][:3])
							corrupt_report.write("{0} sub band {1} was corrupt after NDPPP".format(j, SB_no))
							postcorrupt+=1
							for beam in beams:
								if beam==0:
									target_to_remove=p.replace(j,i).replace(".tmp", "")
								else:
									target_to_remove_sb=SB_no+(diff*beam)
									target_to_remove=p.replace("SB{0}".format('%03d' % SB_no), "SB{0}".format('%03d' % target_to_remove_sb)).replace("SAP000", "SAP00{0}".format(beam)).replace(j, i).replace(".tmp", "")
								log.warning("Deleting {0}...".format(target_to_remove))
								subprocess.call("rm -r {0}".format(target_to_remove), shell=True)
								for k in rsm_bands:
									if target_to_remove in rsm_bands[k]:
										rsm_bands[k].remove(target_to_remove)
										rsm_bands_lens[k]=len(rsm_bands[k])
					corrupt_report.close()
					log.debug("{0} Calibrators after NDPPP = {1}".format(j, calibs[j]))
				
			for b in beams:
				beam=b
				beamc="SAP00{0}".format(b)
				if __name__ == '__main__':
					worker_pool.map(NDPPP_Initial_Multi, targets[i][beamc])
				if not precal:
					targets[i][beamc] = sorted(glob.glob(i+"/*{0}*.tmp".format(beamc)))
				else:
					targets[i][beamc] = sorted(glob.glob(i+"/*{0}*.dppp".format(beamc)))
				if nchans==0:
					temp=pt.table("{0}/SPECTRAL_WINDOW".format(targets[i][beamc][0]), ack=False)
					nchans=int(temp.col("NUM_CHAN")[0])
					log.info("Number of channels in a sub band: {0}".format(nchans))
					temp.close()
				if not os.path.isfile("post_ndppp_corrupt_report.txt"):
					corrupt_report=open("post_ndppp_corrupt_report.txt", 'w')
					corrupt_report.close()
				corrupt_report=open("post_ndppp_corrupt_report.txt", "a")
				log.info("Checking for bad targets in {0} beam {1}...".format(i, beamc))
				checkresults=worker_pool.map(rsmshared.check_dataset, targets[i][beamc])
				for p in checkresults:
					if p!=True:
						targets[i][beamc].remove(p)
						log.warning("Deleting {0}...".format(p))
						subprocess.call("rm -r {0}".format(p), shell=True)
						pp=p.replace(".tmp", "")
						corrupt_report.write("{0} was corrupt after NDPPP".format(pp))
						postcorrupt+=1
						for q in rsm_bands:
							if pp in rsm_bands[q]:
								rsm_bands[q].remove(pp)
								rsm_bands_lens[q]=len(rsm_bands[q])
				corrupt_report.close()
				log.debug("{0} Beam {1} Targets after NDPPP = {2}".format(i, beamc, targets[i][beamc]))
		
			for q in sorted(rsm_bands):
				bandtemp=q.split("_")[-1]
				log.debug("{0} BAND {1} sets: {2}".format(i, bandtemp, rsm_bands[q]))
			
			log.info("Done!")
			# calibrate step 1 process
			if not precal:
				log.info("Calibrating calibrators and transferring solutions for {0} and {1}...".format(i, j))
				calibrate_msss1_multi=partial(rsmhbaf.calibrate_msss1, beams=beams, diff=diff, calparset=calparset, calmodel=calmodel, correctparset=correctparset, dummy=dummy, oddeven=target_oddeven, firstid=firstid_oe)
				if __name__ == '__main__':
					worker_pool.map(calibrate_msss1_multi, calibs[j])
			else:
				log.info("Data is precalibrated - calibrator calibration has been skipped")
		
		log.info("Done!")
		#Combine the bands
		log.info("Creating Bands for all sets...")
		rsm_bandsndppp_multi=partial(rsmshared.rsm_bandsndppp, rsm_bands=rsm_bands)
		if __name__ == '__main__':
			worker_pool.map(rsm_bandsndppp_multi, sorted(rsm_bands.keys()))
		log.info("Done!")
		
		log.info("Performing phaseonly calibration (and flagging if selected) on all sets...")
		# calibrate step 2 process
		tocalibrate=sorted(glob.glob("L*/L*_SAP00?_BAND*.MS.dppp.tmp"))
		calibrate_msss2_multi=partial(rsmshared.calibrate_msss2, phaseparset=phaseparset, flag=flag, toflag=toflag, autoflag=autoflag, create_sky=create_sky, skymodel=skymodel)
		if __name__ == '__main__':
			worker_pool.map(calibrate_msss2_multi, tocalibrate)
		proc_target_obs=sorted(glob.glob("L*/L*_SAP00?_BAND*.MS.dppp"))
		log.info("Done!")
		
		if peeling:
			log.info("Peeling process started on all sets...")
			peeling_steps_multi=partial(rsmshared.peeling_steps, shortpeel=shortpeel, peelsources=peelsources_todo, peelnumsources=peelnumsources, fluxlimit=peelfluxlimit,
			skymodel=skymodel, create_sky=create_sky)
			for t in target_obs:
				os.mkdir(os.path.join(t, "prepeeled_sets"))
			if __name__=='__main__':
				# pool_peeling=mpl(processes=n)
				worker_pool.map(peeling_steps_multi, proc_target_obs)
			log.info("Done!")
		
		if postbbs==True:
			log.info("Post-bbs clipping process started on all sets...")
			post_bbs_multi=partial(rsmshared.post_bbs, postcut=postcut)
			if __name__ == '__main__':
				# pool_postbbs = Pool(processes=n)
				worker_pool.map(post_bbs_multi, proc_target_obs)
			log.info("Done!")
		
		#----------------------------------------------------------------------------------------------------------------------------------------------
		#																Final Concat Step for MSSS style
		#----------------------------------------------------------------------------------------------------------------------------------------------
		correct=nchans*subsinbands
		for be in beams:
			log.info("Final conatenate process started...")
		# 	# snapshot_concat_multi=partial(rsmshared.snapshot_concat, beam=be)	#Currently cannot combine all bands in a snapshot (different number of subands)
			final_concat_multi=partial(rsmhbaf.final_concat, beam=be, target_obs=target_obs, correct=correct)
			if __name__ == '__main__':
				worker_pool.map(final_concat_multi, rsm_band_numbers)
		# 		pool_concat = Pool(processes=5)
		# 		# pool_snapconcat = Pool(processes=5)
		# 		# pool_snapconcat.map(snapshot_concat_multi, sorted(target_obs))
		# 		pool_concat.map(final_concat_multi, rsm_band_numbers)
		# 	print "Done!"

		#----------------------------------------------------------------------------------------------------------------------------------------------
		#																Imaging Step
		#----------------------------------------------------------------------------------------------------------------------------------------------

		#Loops through each group folder and launches an AWimager step or CASA
		if imaging_set:
			#Switch to new awimager if needed (for rsm-mainline)
			if chosen_environ=='rsm-mainline':
				awimager_environ=rsmshared.convert_newawimager(os.environ.copy())
				# print awimager_environ
			else:
				awimager_environ=os.environ.copy()
			
			# globterms=["L*/L*BAND*.MS.dppp", "SAP00*BAND*_FINAL.MS"]		
			toimage=sorted(glob.glob("L*/L*BAND*.MS.dppp"))
			log.info("Starting imaging process with AWimager...")
			#Need to create sky model data
			image_file=open("parsets/aw.parset", 'r')
			aw_sets=image_file.readlines()
			image_file.close()
			aw_sets=[setting.replace(" ", "") for setting in aw_sets]
			mask_size=""
			to_remove=[]
			userthresh=0.0
			userpad=1.0
			for s in aw_sets:
				if "ms=" in s or "image=" in s or "UVmax=" in s:
					to_remove.append(s)
				if "npix=" in s or "cellsize=" in s or "data=" in s:
					mask_size+=" "+s.strip('\n')
				if "niter=" in s:
					niters=int(s.split("=")[1])
					to_remove.append(s)
				if "threshold=" in s:
					userthresh=float(s.split("=")[1].replace("Jy", ""))
					to_remove.append(s)
				if "padding=" in s:
					userpad=float(s.split("=")[1])
			for j in to_remove:
				aw_sets.remove(j)
			log.info("Maximum baseline to image: {0} {1}".format(maxb, maxbunit))
			if mask:
				create_mask_multi=partial(rsmshared.create_mask, mask_size=mask_size, toimage=toimage)
				if __name__ == '__main__':
					worker_pool.map(create_mask_multi,beams)
			AW_Steps_multi=partial(rsmshared.AW_Steps, aw_sets=aw_sets, maxb=maxb, aw_env=awimager_environ, niter=niters, automaticthresh=automaticthresh,
			bandsthreshs_dict=bandsthreshs_dict, initialiter=initialiters, uvORm=maxbunit, usemask=mask, userthresh=userthresh, mos=mosaic)
			if __name__ == '__main__':
				pool = Pool(processes=2)
				pool.map(AW_Steps_multi,toimage)
			log.info("Done!")

			log.info("Tidying up imaging...")
			for i in target_obs:
				os.chdir(i)
				os.mkdir("images")
				subprocess.call("mv *.fits images", shell=True)
				subprocess.call("mv *.model *.residual *.psf *.restored *0.avgpb *.img0.spheroid_cut* *.corr images", shell=True)
				os.chdir("..")
			log.info("Creating averaged images...")
			average_band_images_multi=partial(rsmshared.average_band_images, beams=beams)
			if __name__=='__main__':
				worker_pool.map(average_band_images_multi, target_obs)
			if mosaic:
				create_mosaic_multi=partial(rsmshared.create_mosaic, band_nums=rsm_band_numbers, chosen_environ=chosen_environ, pad=userpad, avgpbr=avpbrad)
				pool=Pool(processes=len(rsm_band_numbers))
				pool.map(create_mosaic_multi, target_obs)
				
		#----------------------------------------------------------------------------------------------------------------------------------------------
		#																End of Process
		#----------------------------------------------------------------------------------------------------------------------------------------------
 
		#Finishes up and moves the directory if chosen, performing checks
		log.info("Tidying up...")
		os.mkdir("final_datasets")
		subprocess.call("mv SAP00*BAND*_FINAL.MS.dppp final_datasets", shell=True)
		if not precal:
			for c in calib_obs:
				subprocess.call("mkdir {0}/datasets {0}/parmdb_tables".format(c), shell=True)
				subprocess.call("mv {0}/*.pdf {0}/plots".format(c), shell=True)
				subprocess.call("mv {0}/*.tmp {0}/datasets".format(c), shell=True)
				subprocess.call("mv {0}/*.parmdb {0}/parmdb_tables".format(c), shell=True)
		os.mkdir("Calibrators")
		mv_calibs=["mv",]+sorted(calib_obs)
		mv_calibs.append("Calibrators")
		subprocess.call(mv_calibs)
		if precal:
			subprocess.call(["rm", "-r", "Calibrators"])
		for t in target_obs:
			subprocess.call("mv {0}/*_uv.MS.dppp {0}/datasets".format(t), shell=True)
			if autoflag:
				subprocess.call("mv {0}/*.stats {0}/*.pdf {0}/*.tab {0}/flagging".format(t), shell=True)
		subprocess.call(["rm","-r","sky.calibrator","sky.dummy"])
		if postcorrupt==0:
			subprocess.call(["rm","post_ndppp_corrupt_report.txt"])

		if destroy:
			log.warning("Lightweight Mode Selected, now cleaning up target datasets...")
			# for c in calib_obs:
			# 	subprocess.call(["rm", "-r", "Calibrators/{0}/parmdb_tables".format(c)])
			for t in target_obs:
				subprocess.call("rm -r {0}/datasets".format(t), shell=True)

		log.info("All processed successfully!")
		log.info("Results can be found in {0}".format(newdirname))
	
		end=datetime.datetime.utcnow()
		date_time_end=end.strftime("%d-%b-%Y %H:%M:%S")
		tdelta=end-now

		if mail==True:
			em.send_email(emacc,user_address,"rsmpp Job Completed","{0},\n\nYour job {1} has been completed - finished at {2} UTC with a runtime of {3}".format(user,newdirname, date_time_end, tdelta))

		os.chdir("..")
		log.info("Run finished at {0} UTC with a runtime of {1}".format(date_time_end, str(tdelta)))
		subprocess.call(["cp", "rsmpp.log", "{0}/rsmpp_{0}.log".format(newdirname)])
	except Exception, e:
		log.exception(e)
		end=datetime.datetime.utcnow()
		date_time_end=end.strftime("%d-%b-%Y %H:%M:%S")
		tdelta=end-now
		subprocess.call(["cp", os.path.join(root_dir, "rsmpp_hba.log"), "rsmpp_hba_CRASH.log".format(newdirname)])
		if mail==True:
			em.send_email(emacc,user_address,"rsmpp Job Error","{0},\n\nYour job {1} crashed with the following error:\n\n{2}\n\nTime of crash: {3}".format(user,newdirname,e, end))
			em.send_email(emacc,"adam.stewart@astro.ox.ac.uk","rsmpp Job Error","{0}'s job '{1}' just crashed with the following error:\n\n{2}\n\nDirectory: {3}\n\nTime of crash: {4}".format(user,newdirname,e,root_dir,end))
				
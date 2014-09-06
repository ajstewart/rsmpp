#Version 2.00

import os, subprocess,time, glob, logging, sys
import pyrap.tables as pt
import rsmppsharedfuncs as rsmshared

rootpath=os.path.realpath(__file__)
rootpath=rootpath.split("/")[:-1]
rootpath="/"+os.path.join(*rootpath)

tools={"editparmdb":os.path.join(rootpath, "tools", "edit_parmdb", "edit_parmdb.py"),
"solplot":os.path.join(rootpath, "tools", "plotting", "solplot.py"),
}

log=logging.getLogger("rsm")

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Function Definitions
#----------------------------------------------------------------------------------------------------------------------------------------------

def check_targets(i, beam, targets, targets_corrupt, rsm_bands, rsm_band_numbers, rsm_bands_lens, missing_calibrators, data_dir, diff, missingfile, subsinbands, calibbeam):
	"""
	Checks all target observations, works out if any are missing and then organises into bands.
	"""
	localmiss=0
	beamselect="SAP00{0}".format(beam)
	log.info("Checking {0} Beam SAP00{1}...".format(i,beam))
	targlob=os.path.join(data_dir,i,"*{0}*.MS.dppp".format(beamselect))
	targets[i][beamselect]=sorted(glob.glob(targlob))
	if len(targets[i][beamselect])<1:
		log.critical("Cannot find any measurement sets in directory {0} !".format(os.path.join(data_dir,i)))
		sys.exit()
	targets_first=int(targets[i][beamselect][0].split('SB')[1][:3])
	targets_last=int(targets[i][beamselect][-1].split('SB')[1][:3])
	target_range=range(0+(beam*diff), diff+(beam*diff))
	temp=[]
	toremove=[]
	for bnd in rsm_band_numbers:
		rsm_bands["{0}_{1}_{2}".format(i, beamselect, bnd)]=[]
	for t in targets[i][beamselect]:
		target_msname=t.split("/")[-1]
		try:
			test=pt.table(t, ack=False)
		except:
			log.warning("Target {0} is corrupt!".format(target_msname))
			time.sleep(1)
			targets_corrupt[i].append(t)
			toremove.append(t)
			missingfile.write("Measurement set {0} corrupted from observation {1}\n".format(target_msname, i))
		else:
			SB=int(t.split('SB')[1][:3])
			if calibbeam < beam:
				SB_cal=int(t.split('SB')[1][:3])-(beam*diff)
			else:
				SB_cal=int(t.split('SB')[1][:3])+(diff*(calibbeam-beam))
			temp.append(SB)
			if SB_cal in missing_calibrators[i]:
				toremove.append(t)
				miss=True
			else:
				miss=False
			if miss==False:
				if calibbeam < beam:
					target_bandno=int(SB_cal/subsinbands)
				else:
					target_bandno=int((SB_cal-(diff*calibbeam))/subsinbands)
				if target_bandno>=len(rsm_band_numbers):
					target_bandno-=1
				rsm_bands[i+"_"+beamselect+"_{0}".format(target_bandno)].append(i+"/"+t.split("/")[-1])
	for s in target_range:
		if s not in temp:
			missingfile.write("Sub band {0} missing from observation {1}\n".format(s, i))
			localmiss+=1
	for j in toremove:
		targets[i][beamselect].remove(j)
	for k in rsm_bands:
		rsm_bands_lens[k]=len(rsm_bands[k])
	return localmiss

def calibrate_msss1(Calib, beams, diff, calparset, calmodel, correctparset, dummy, calibbeam, mode):
	"""
	Function that performs the full calibrator calibration and transfer of solutions for HBA and LBA. Performs \
	the calibration and then shifts the corrected data over to a new data column.
	"""
	calibsplit=Calib.split('/')
	curr_obs=calibsplit[0]
	calib_name=calibsplit[-1]
	curr_SB=int(Calib.split("SB")[1][:3])
	log.info("Calibrating calibrator {0}...".format(calib_name))
	subprocess.call("calibrate-stand-alone --replace-parmdb --sourcedb sky.calibrator {0} {1} {2} > {3}/logs/calibrate_cal_{4}.txt 2>&1".format(Calib,calparset,calmodel, curr_obs, calib_name), shell=True)
	log.info("Zapping suspect points for {0}...".format(calib_name))
	subprocess.call("{0} --sigma=1 --auto {1}/instrument/ > {2}/logs/edit_parmdb_{3}.txt 2>&1".format(tools["editparmdb"], Calib, curr_obs, calib_name), shell=True)
	log.info("Making diagnostic plots for {0}...".format(calib_name))
	subprocess.call("{0} -q -m -o {3}/{1} {2}/instrument/ > {3}/logs/solplot.log 2>&1".format(tools["solplot"], calib_name, Calib, curr_obs),shell=True)
	for beam in beams:
		if beam<calibbeam:
			target_subband=curr_SB-(diff*(calibbeam-beam))
			target=Calib.replace("SB{0}".format('%03d' % curr_SB), "SB{0}".format('%03d' % target_subband)).replace("SAP00{0}".format(calibbeam), "SAP00{0}".format(beam))
		else:
			target_subband=curr_SB+(diff*beam)
			target=Calib.replace("SB{0}".format('%03d' % curr_SB), "SB{0}".format('%03d' % target_subband)).replace("SAP00{0}".format(calibbeam), "SAP00{0}".format(beam))
		target_name=target.split('/')[-1]
		log.info("Transferring calibrator solutions to {0}...".format(target_name))
		subprocess.call("calibrate-stand-alone --sourcedb sky.dummy --parmdb {0}/instrument {1} {2} {3} > logs/calibrate_transfer_{4}.txt 2>&1".format(Calib, target, correctparset, dummy, target_name), shell=True)
		rsmshared.shiftndppp(target, curr_obs, target_name)


# def snapshot_concat(i, beam):
# 	"""
# 	Simply uses concat.py to concat all the BANDX together in each snapshot.
# 	"""
# 	log.info("Combining {0} SAP00{1} datasets...".format(i,beam))
# 	subprocess.call("~as24v07/scripts/concat.py {0}/{0}_SAP00{1}_ALLBANDS.MS {0}/L*SAP00{1}_BAND??.MS.dppp > {0}/logs/concat_SAP00{1}_allbands.log 2>&1".format(i,beam), shell=True)
# 
# def final_concat(b, beam, target_obs, rsm_bands_lens):
# 	"""
# 	Simply uses concat.py to concat all the BANDX together into a final set.
# 	"""
# 	datasetstocon=sorted(glob.glob("L*/L*SAP00{0}_BAND{1}.MS.dppp".format(beam,'%02d' % b)))
# 	numbers=[]
# 	datasets={}
# 	for i in target_obs:
# 		num=rsm_bands_lens[i+"_"+"SAP00{0}".format(beam)+"_{0}".format(b)]
# 		numbers.append(num)
# 		datasets[i]=num
# 	cnt=Counter()
# 	for a in numbers:
# 		cnt[a]+=1
# 	correct=int(cnt.most_common(1)[0][0])
# 	for i in datasets:
# 		if datasets[i]!=correct:
# 			datasetstocon.remove("{0}/{0}_SAP00{1}_BAND{2}.MS.dppp".format(i, beam, '%02d' % b))
# 	concat_commd="/home/as24v07/scripts/concat.py SAP00{0}_BAND{1}_FINAL.MS".format(beam,'%02d' % b)
# 	for i in datasetstocon:
# 		concat_commd+=" {0}".format(i)
# 	concat_commd+=" > logs/concat_SAP00{0}_band{1}.log 2>&1".format(beam,'%02d' % b)
# 	log.info("Combining Final BEAM {0} BAND{1}...".format(beam, '%02d' % b))
# 	subprocess.call(concat_commd, shell=True)
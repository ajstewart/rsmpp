#! /usr/bin/env python

import os

for i in [0,1,2,3,4,5,6,7]:
	os.chdir('./band'+str(i))

	# remake peeling.parset
	os.system('/home/mrbell/bin/peeling_new.py -n 5 -l 1. -p peeling.parset -m peeling.skymodel -v -i ./msfiles/L53295_SAP000_BAND'+str(i)+'.MS | tee peeling_py.log')
	# run do_peel_cal.py
	os.system('/home/mrbell/bin/do_peel_cal_new.py | tee do_peel_cal.log')
	# make an aw.parset
	#os.system('rm -rf CONCAT_PEELED_rob+0.0_maxbl3000_fov10.img*')
	#os.system('/home/mrbell/bin/aw.py -v -s \'none\' -b 12000 -o aw_peeled_12km.parset CONCAT_PEELED.MS | tee awpy_peeled_12km.log')
	#os.system('/home/mrbell/bin/aw.py -v -s \'none\' -b 12000 -o aw_prepeel_12km.parset CONCAT_PREPEEL.MS | tee awpy_prepeel_12km.log')
	# run awimager
	#os.system('awimager aw_peeled_12km.parset | tee awimager_peeled_12km.log')
	#os.system('awimager aw_prepeel_12km.parset | tee awimager_prepeel_12km.log')
	os.chdir('../')

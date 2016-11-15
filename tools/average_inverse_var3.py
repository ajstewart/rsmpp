#!/usr/bin/env python

# Quick & dirty inverse-variance weighted image averaging.
# John Swinbank, 2010-07-10, Adam Stewart, Jess Broderick
# Not quite so dirty now with clipping method used to calculate noise for each image.

import sys, optparse, os, subprocess
import numpy as np  
from pyrap.images import image
from astropy.io import fits

vers="3.1"
usage = "Usage: python %prog [options] <output_name> <fits1> <fits2> [<fits3> ...]"
description="Improved image averaging script which uses inverse variance weighting. The RMS noise is calculated using a clipping method for each image.\
Average beam information is also added to the final fits image automatically."
parser = optparse.OptionParser(usage=usage,version="%prog v{0}".format(vers), description=description)
parser.add_option("-w", "--overwrite", dest="overwrite", action="store_true", default=False, help="Use this option to overwrite any exisiting averaged images of the same name [default: %default]")
(options, args) = parser.parse_args()

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

#Arg values and checks
x=len(args[1:])
if x < 2:
	print "Please define at least two fits files to average together."
	sys.exit()
outputname=args[0]
fits_name=outputname+".fits"
img_name=outputname+".img"
fexist=False
y=args[1:] 

if os.path.isfile(fits_name):
	if fits_name in y:
		y.remove(fits_name)
		x=len(y)
	if options.overwrite==True:
		os.remove(fits_name)
	else:
		fexist=True

if os.path.isdir(img_name):
	if options.overwrite==True:
		subprocess.call(["rm","-r",img_name])
	else:
		fexist=True
		
if fexist==True:
	print "Averaged image with the same name already exists - please run again with -w option if you wish to overwrite."
	sys.exit()
		 
v=[0]*x  
w=[0]*x
bmaj, bmin, bpa=[], [], []

#Weighting calculation
print "Averaging the following fits files:\n{0}".format(y)
 
print "Calculating weights and obtaining beam info..."

for i in range(0,x): 
	fln=fits.open(y[i]) 
	rawdata=fln[0].data
	angle=fln[0].header['obsra']
	bscale=fln[0].header['bscale']
	bmaj.append(float(fln[0].header["BMAJ"]))
	bmin.append(float(fln[0].header["BMIN"]))
	bpa.append(float(fln[0].header["BPA"]))
	rawdata=rawdata.squeeze()
	rawdata=rawdata*bscale
	while len(rawdata) < 20:
	    rawdata = rawdata[0]
	X,Y = np.shape(rawdata)
	rawdata = rawdata[Y/3:2*Y/3,X/3:2*X/3]
	orig_raw = rawdata
	med, std, mask = Median_clip(rawdata, full_output=True, ftol=0.0, max_iter=10, sigma=4)
	rawdata[mask==False] = med
	v[i]=std
	fln.close() 
  
for i in range(0,x): 
  w[i]=1/v[i] 
 
print "Calculated weights:\n{0}".format(w)

#Gather images 
image_data = [ 
    image(file).getdata() for file in y 
]

#Write result
result = np.average( 
    image_data, axis=0, 
    weights=w 
) 
 
output = image( 
    img_name, values=result, 
    coordsys=image(y[-1]).coordinates() 
)

output.tofits(fits_name)

#Calculate beam information
print "Adding average beam information to final fits..."
av_bmaj=np.average(bmaj)
av_bmin=np.average(bmin)
av_bpa=np.average(bpa)

print "Average Beam Info - {0:.2f} arcsec x {1:.2f} arcsec (BPA {2:.2f})".format(av_bmaj*3600., av_bmin*3600., av_bpa)
hdulist = fits.open(fits_name, mode='update')
prihdr = hdulist[0].header
prihdr['BMAJ'] = av_bmaj
prihdr['BMIN'] = av_bmin
prihdr['BPA'] = av_bpa
prihdr['BUNIT'] = "JY/BEAM"
hdulist.flush()
hdulist.close()
print "Done!"

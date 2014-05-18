#!/usr/bin/env python

import sys
import numpy
from parmdb.parmdb import WriteableParmDB

if __name__ == "__main__":
    try:
        pdb = WriteableParmDB(sys.argv[1])
        new_freq = numpy.float64(sys.argv[2])
    except:
       print "Usage: shift_parmdb.py <parmdb> <frequency>"
       print "<frequency> in Hz; corresponds to the centre of the target band"
    else:
        name = pdb.getNames()[0]
        for key, value in pdb.getValuesGrid('*').iteritems():
            pdb.setValues(
                key,
                value['values'],
                new_freq,
                value['freqwidths'][0],
                value['times'][0],
                value['timewidths'][0]
            )

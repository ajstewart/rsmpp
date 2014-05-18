#!/usr/bin/env python

import sys
from parmdb.stationgain import StationGain
from edit_parmdb import list_stations

if __name__ == "__main__":
    pdbfile = sys.argv[1]
    stations = list_stations(pdbfile)
    for station in stations:
        stationgain = StationGain(pdbfile, station)
        for value in stationgain.itervalues():
            value.phase = value.phase * 0
        stationgain.writeout()

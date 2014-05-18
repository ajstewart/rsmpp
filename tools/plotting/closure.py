#!/usr/bin/env python
import os
import optparse
import sys
import numpy.ma
import pylab as lb
import pyrap.tables as pt

#Print closure phase vs time for three selected antennas and channel range/polarisation

def getTable(t, firstTime, lastTime, antenna1, antenna2):
    # Get the table for the selected time range and antennas-pair 
    t1=t.query('TIME >= ' + str(firstTime) + ' AND TIME <= ' + str(lastTime) + ' AND ANTENNA1= '+str(antenna1) +' '+'AND ANTENNA2= '+str(antenna2))
    print 'Getting data from baseline '+str(t1.getcell("ANTENNA1",0))+'-' +str(t1.getcell("ANTENNA2",0))
    return t1
    
def getTime(table):
    tmp = table.getcol('TIME')
    if tmp != None:
        return numpy.array(tmp-tmp.min(),dtype=numpy.float)
    return None

def getPhase(table, column, flagCol, showFlags, stokes, channels, unwrap):
    
    tmp = table.getcol(column)
    flg = table.getcol(flagCol)
    
    if tmp != None:
        if showFlags:
            tmp2 = numpy.ma.array(tmp, dtype=None, mask=False)
        else:
            tmp2 = numpy.ma.array(tmp, dtype=None, mask=flg)
        
        tmp2[numpy.isnan(tmp2)]=numpy.ma.masked
        
        if stokes:
            tmp2 = numpy.ma.transpose(numpy.ma.array([tmp2[:,:,0]+tmp2[:,:,3],tmp2[:,:,0]-tmp2[:,:,3],tmp2[:,:,1]+tmp2[:,:,2],numpy.complex(0.,-1.)*(tmp2[:,:,2]-tmp2[:,:,1])],dtype=None,mask=tmp2.mask),(1,2,0))
        
        avgvals = numpy.ma.array(numpy.ma.mean(tmp2[:,channels[0]:channels[1],:],axis=1), mask=numpy.ma.mean(tmp2[:,channels[0]:channels[1],:],axis=1).mask)
        
        tmp3 = numpy.ma.arctan2(avgvals.imag,avgvals.real)
        
        return tmp3
    return None

# Main closure method
def main(options):
    
    # Check the inputms
    if (options.inms == ''):
        print 'Error: You must specify an input Measurement Set.'
        print '       Use "./closure.py -h" to get help.'
        sys.exit()
    elif not os.path.isdir(options.inms):
        print 'Error: Measurement Set not found.'
        sys.exit()
    
    # Check the antennas to use
    antToPlot = options.antennas.split(',')
    if len(antToPlot) != 3:
        print 'Error: Please, provide antennas in groups of 3, e.g.: 0,1,2'
        sys.exit()
        
    # Check the timeslots to use
    timeslots = options.timeslots.split(',')
    if len(timeslots) != 2:
        print 'Error: Timeslots format is start,end'
        sys.exit()
    
    # Format the antToPlot and timeslots
    for i in range(len(antToPlot)):
        antToPlot[i] = int(antToPlot[i])
    antToPlot = sorted(antToPlot)
    
    for i in range(len(timeslots)): 
        timeslots[i] = int(timeslots[i])
    
    # Get the channels to use
    channels = options.channels.split(',')
    if len(channels) != 2:
        print 'Error: Channels format is start,end'
        sys.exit()
    for i in range(len(channels)): channels[i] = int(channels[i])
    if channels[1] == -1:   
        channels[1] = None # last element even if there is only one
    else:
        channels[1] += 1
        
    # Get the polarization to plot
    polarizations = options.polar.split(',')
    for i in range(len(polarizations)):
        polarizations[i] = int(polarizations[i])
    
    #Open table
    t=pt.table(options.inms)
    
    # PRINT MS INFORMATION
    firstTime = t.getcell("TIME", 0)
    lastTime = t.getcell("TIME", t.nrows()-1)
    intTime = t.getcell("INTERVAL", 0)
    print 'Integration time:\t%f sec' % (intTime)
    nTimeslots = (lastTime - firstTime) / intTime
    if timeslots[1] == -1:
            timeslots[1] = nTimeslots
    else:
            timeslots[1] += 1
    print 'Number of timeslots:\t%d' % (nTimeslots)
    # open the antenna and spectral window subtables
    tant = pt.table(t.getkeyword('ANTENNA'), readonly=True, ack=False)
    tsp = pt.table(t.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False)
    numChannels = len(tsp.getcell('CHAN_FREQ',0))
    print 'Number of channels:\t%d' % (numChannels)
    print 'Reference frequency:\t%5.2f MHz' % (tsp.getcell('REF_FREQUENCY',0)/1.e6)

    # Station names
    usedAntNames = ''
    antList = tant.getcol('NAME')
    print 'Station list (starred stations will be used):'
    for i in range(len(antList)):
            star = ' '
            if i in antToPlot: 
                star = '*'
                usedAntNames += ', ' + antList[i]
            print '%s %2d\t%s' % (star, i, antList[i])
    
    # Bail if we're in query mode
    if options.query:
        sys.exit()

    if options.stokes:
        polLabels = ['I','Q','U','V']
    else:
        polLabels = ['XX','XY','YX','YY']

    initialTime = firstTime+timeslots[0]*intTime
    finalTime = firstTime+timeslots[1]*intTime
    
    # Load data for the first baseline
    ta = getTable(t, initialTime, finalTime, antToPlot[0], antToPlot[1])
    time = getTime(ta)
    phasea = getPhase(ta, options.column, options.colflag, options.flag, options.stokes, channels, options.wrap)

    # Load the data for the second baseline (not time required in this case)
    tb = getTable(t, initialTime, finalTime, antToPlot[1], antToPlot[2])
    phaseb = getPhase(tb, options.column, options.colflag, options.flag, options.stokes, channels, options.wrap)
    
    # Load the data for the third baseline (not time required in this case)
    tc = getTable(t, initialTime, finalTime, antToPlot[0], antToPlot[2])
    phasec = getPhase(tc, options.column, options.colflag, options.flag, options.stokes, channels, options.wrap)
    
    closure = numpy.ma.array(phasea + phaseb - phasec, mask=(phasea + phaseb - phasec).mask)
    
    if options.wrap:
        for i in range(len(closure)):
            for j in polarizations:
                isOk = False
                closureElement = closure[i][j]
                while not isOk:
                    if closureElement > numpy.pi:
                        closureElement -= 2*numpy.pi
                    elif closureElement<-numpy.pi:
                        closureElement += 2*numpy.pi
                    else:
                        isOk = True
                        closure[i][j] = closureElement   
        
    lb.figure(1)
    lb.clf()
       
    titlestring = "Closure Phase:" + usedAntNames[1:]     
    ylabelstring = "Closure Phase/Radians"
    xlabelstring="Time"
    
    for i in polarizations:
        
        print 'Plotting polarization ' +  polLabels[i]
        tmpvals = closure[:,i]
        lb.plot(time[~tmpvals.mask], tmpvals[~tmpvals.mask],'.',label=polLabels[i])
    
    lb.ylabel(ylabelstring)
    lb.xlabel(xlabelstring)
    lb.title(titlestring)
    lb.legend()
                 
    lb.show()
   
# The option parser   
 
opt = optparse.OptionParser()
opt.add_option('-i','--inms',help='Input MS [no default]',default='')
opt.add_option('-t','--timeslots',help='Timeslots to use (comma separated and zero-based: start,end[inclusive]) [default 0,-1 = all] Negative values work like python slicing, but please note that the second index here is inclusive. If plotting channel or frequency on x-axis, this parameter sets the time averaging interval.',default='0,-1')
opt.add_option('-e','--antennas',help='The 3 Antennas to use (comma separated list, zero-based). [default 0,1,2] Use -q to see a list of available antennas. Only antennas in this list are plotted.',default='0,1,2',type='string')
opt.add_option('-q','--query',help='Query mode (quits after reading MS Information, use for unfamiliar MSs) [default False]',default=False,action='store_true')
opt.add_option('-p','--polar',help='Polarizations to plot (it does not convert, so use integers as in the MS) [default 0,1,2,3]',default='0,1,2,3')
opt.add_option('-s','--channels',help='Channels to use (comma separated and zero-based: start,end[inclusive]) [default 0,-1 = all] Negative values work like python slicing, but please note that the second index here is inclusive.',default='0,-1')
opt.add_option('-f','--flag',help='Show flagged data? [default False]',default=False,action='store_true')
opt.add_option('-k','--stokes',help='Convert to Stokes IQUV? [default False]',default=False,action='store_true')
opt.add_option('-w','--wrap',help='Unwrap phase? [default False]',default=False,action='store_true')
opt.add_option('-c','--column',help='Column to plot [default DATA]',default='DATA')
opt.add_option('-g','--colflag',help='Column that contains flags [default FLAG]',default='FLAG')
options, arguments = opt.parse_args()

# Load the main method

def loader(options):
        main(options)
loader(options)

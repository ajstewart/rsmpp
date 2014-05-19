#!/usr/bin/env python
"""
Plot statistics obtained with ASCIIStats and select bad stations. It makes 3 analysis.

 1 - For each station we get the median (or mean) of the related baselines (of amp by default) 
     and we do the median (always median) of those. We mark as candidates the stations of which baselines are further than certain threshold (tm1).
     Then, for each station we count how many other stations have selected it as candidate. It this number is more than a 
     certain threshold (tm2) we mark this station as bad. 
     We can do the later for some polarizations, then we do an average of the polarizations and if this number os higher than 
     a certain threshold (tm3) we also select this station as bad
     
 2 - We do the same for std, with different thresholds (ts1,ts2,ts3), polarizations  (in this case default is real). In this case the std values 
     have a correction (1/sqrt(2) for Dutch-international baselines and 1/2 for international-international baselines). We select candidates
     and bad stations as in the mean/median case
     
 3 - For each station we sum the num (number used samples, i.e. not flag) of the related baselines. We get the maximum 
     (station with higher sum of num). The stations which sum-of-num are further than certain threshold (tn1) are marked as bad. We average the
     polarizations as in the other cases and select as given a certain threshold (tn2)

Written by Oscar Martinez

version 0.1 28/02/2012: Initial version
version 0.2 02/03/2012: Big update, simultaneous std and num study with median/mean
                        possibility to output in file instead of windows
"""
import optparse, os
import numpy

version_string = 'v0.2, 2 March 2012\nWritten by Oscar Martinez'
print 'statsplot.py',version_string
print ''

POL_NAMES_INDEXES = {'XX':0,'XY':1,'YX':2,'YY':3}
POL_INDEXES_NAMES = {0:'XX',1:'XY',2:'YX',3:'YY'}
NUM_ANALYSIS = 3

def updateMinMax(stat,minStat,maxStat,analysisIndex):
    if maxStat == None or minStat == None:
        (minStat[analysisIndex],maxStat[analysisIndex]) = (stat,stat)
    else:
        (minStat[analysisIndex],maxStat[analysisIndex]) = (min([stat, minStat[analysisIndex]]), max([stat, maxStat[analysisIndex]]))

def main(opts):
    # Check the input file path
    absPath = opts.input
    if absPath != '':
        if not os.path.isfile(absPath):
            print 'Error: ' + absPath + ' does not exist'
            exit()
    import matplotlib
    if opts.out != '':
        # We use this backend to guarantee no X window will be used
        matplotlib.use('Agg')
        ofile = opts.out + '.tab'
        if os.path.isfile(ofile):
            print 'Removing: ' + ofile
            os.system('rm ' + ofile)
    import matplotlib.pyplot as plt
    
    analysisname = [None,'std','num']
    analysisshortname = [None, 's', 'n']
    # Check wheter we want a median or mean analysis
    if opts.mean:
        analysisname[0] = 'mean'
        analysisshortname[0] = 'me'
    else:
        analysisname[0] = 'median'
        analysisshortname[0] = 'md'

    polarizationsset = set([])
    # Get the polarizations for each one of the analysis
    analysispolars = (opts.mpolar.split(','),opts.spolar.split(','),opts.npolar.split(','))
    
    for polars in analysispolars:
        for i in range(len(polars)):
            polars[i] = int(polars[i])
            polarizationsset.add(polars[i])
    
    # This list contains the polarizations that are needed for all the analysis 
    polarizations = sorted(list(polarizationsset))
    
    # Get the thresholds for each one of the analysis
    analysisthresholds = (opts.mthres.split(','),opts.sthres.split(','),opts.nthres.split(','))
    for thress in analysisthresholds:
        for i in range(len(thress)):
            thress[i] = float(thress[i])
    
    # Get which complex coordinate to use for the several analysis
    (mcoord,scoord) = (opts.mcoord,opts.scoord)
    
    # Initialize to None the min and max values of the stats (for plotting purposes)
    dictName = {}
    lines = open(absPath, 'r').read().split('\n')
    if not len(lines):
        print 'Error: ' + absPath + ' is empty'
    headerfields = lines[0].split()
    
    analysisstatsindex = (headerfields.index(mcoord+'_'+analysisname[0]), headerfields.index(scoord+'_'+analysisname[1]))
    
    data = []
    for polars in analysispolars:
        data_analysis = {}
        for polar in polars:
            data_analysis[polar]= {}
        data.append(data_analysis)
    
    (minStat,maxStat)=([None,None,None],[None,None,None])
    
    for i in range(1,len(lines)):
        line = lines[i]
        if line != '':
            fields = line.split()
            fIndex = 8
            (sbfreq,ant1,ant2,ant1Name,ant2Name,pol,num,numflag) = fields[:fIndex]
            polIndex = POL_NAMES_INDEXES[pol]
            if not sbfreq.startswith('#') and int(num) and polIndex in polarizations:
                (ant1,ant2)=(int(ant1),int(ant2))
                dictName[ant1] = ant1Name
                dictName[ant2] = ant2Name
                # We set the data for the analysis
                analysiscounter = 0
                for analysisstat in (float(fields[analysisstatsindex[0]]),float(fields[analysisstatsindex[1]]), int(num)):
                    if polIndex in analysispolars[analysiscounter]:
                        data[analysiscounter][polIndex][(ant1,ant2)] = analysisstat 
                        updateMinMax(analysisstat,minStat,maxStat,analysiscounter)
                    analysiscounter += 1
                                
    # Get the indexes of the stations    
    stationsIndexes = sorted(dictName)
    numstations = len(stationsIndexes)
    # Initialize the plots for the analysis
    params = {'axes.labelsize': 7,
            'text.fontsize': 6,
            'legend.fontsize': 6,
            'xtick.labelsize': 6,
            'ytick.labelsize': 6,
            'figure.figsize': (14.,7.)}
    matplotlib.rcParams.update(params)
    plts = []
    for i in (0,1):        
        plts.append(plt.figure())
        plts[-1].suptitle(absPath + ' (Blue: OK, Red:Bad_baseline, Purple: Bad_station_pol, Black:Bad_station_avg) using ' + analysisname[i],fontsize=8)

    # Initialize discarded stattions counters for the 3 analysis
    discStations = []
    for polars in analysispolars:
        discStations_analysis = {}
        for stationIndex in stationsIndexes:
            discStations_analysis[stationIndex] = {}
            for polar in polars:
                discStations_analysis[stationIndex][polar]= 0
        discStations.append(discStations_analysis)
    
    # Initialize the plot info for the several analysis
    plotInfo = []
    for i in range(NUM_ANALYSIS):
        plotInfo.append({})
    
    max_num_mean = {}
    for polar in analysispolars[2]:
        max_num_mean[polar] = 0
    
    # lets compute the x and y values
    for stationIndex in stationsIndexes:
        xvalues = []
        yvalues = []
        for i in range(NUM_ANALYSIS):
            y = {}
            x = {}
            for polar in analysispolars[i]:
                y[polar] = []
                x[polar] = []
            yvalues.append(y)
            xvalues.append(x)
        
        for stationIndex_2 in stationsIndexes:
            if stationIndex != stationIndex_2:
                if stationIndex_2 > stationIndex:
                    ind_1 = stationIndex
                    ind_2 = stationIndex_2
                else:
                    ind_1 = stationIndex_2
                    ind_2 = stationIndex
                for i in range(NUM_ANALYSIS):
                    for polar in analysispolars[i]:
                        # This is the usual baseline format, but sometimes it is the other one
                        if (ind_1,ind_2) in data[i][polar]:                
                            yvalues[i][polar].append(data[i][polar][(ind_1,ind_2)] )
                            xvalues[i][polar].append(stationIndex_2)
                        elif (ind_2,ind_1) in data[i][polar]:
                            yvalues[i][polar].append(data[i][polar][(ind_2,ind_1)] )
                            xvalues[i][polar].append(stationIndex_2)

        # First analysis and second analysis have the same algorithm
        for analysisIndex in (0,1):
            xs = {}
            ys = {}
            dmasks = {}    
            for polarization in analysispolars[analysisIndex]:
                xs[polarization] = numpy.array(xvalues[analysisIndex][polarization])
                ys[polarization] = numpy.array(yvalues[analysisIndex][polarization], dtype=numpy.float32)
                ydiff = ys[polarization] - numpy.median(ys[polarization])
                medydiff = numpy.median(numpy.absolute(ydiff))
                dmasks[polarization] = numpy.absolute(ydiff) > (analysisthresholds[analysisIndex][0] * 1.48 * medydiff)
                for stationIndex_3 in xs[polarization][dmasks[polarization]]:
                    discStations[analysisIndex][stationIndex_3][polarization] += 1
                plotInfo[analysisIndex][stationIndex] = (xs, ys, dmasks)
        
        #Third analysis analysis
        analysisIndex = 2
        xs = {}
        ys = {}
        dmasks = {}
        num_mean = {}    
        for polarization in analysispolars[analysisIndex]:
            xs[polarization] = numpy.array(xvalues[analysisIndex][polarization])
            ys[polarization] = numpy.array(yvalues[analysisIndex][polarization], dtype=numpy.float32)
            num_mean[polarization]= ys[polarization].sum() / numstations
            max_num_mean[polarization] = max(max_num_mean[polarization],num_mean[polarization])
        plotInfo[analysisIndex][stationIndex] = (xs, ys, num_mean)
    
    # This is to add info for each station
    stationInfo = {}
    analysisBadCounter = {}
    for stationIndex in stationsIndexes:
        stationInfo[stationIndex] = []
        analysisBadCounter[stationIndex] = 0
    
    
    # Lets make the plots
    # First two plots are same treatment
    for analysisIndex in (0,1):
        fig = plts[analysisIndex]
        sCounter = 0
        nx = len(plotInfo[analysisIndex])
        #nx = numstations
        for stationIndex in stationsIndexes:
            station = dictName[stationIndex]
            (xs, ys, dmasks) = plotInfo[analysisIndex][stationIndex]
            kostation = False            
            w = []
            u = []
            for polarization in analysispolars[analysisIndex]:
                w.append(len(xs[polarization]))
                u.append(discStations[analysisIndex][stationIndex][polarization])
            w = numpy.array(w)
            u = numpy.array(u)
            # Check the average of the polarizations
            avg_ko = False
            if (((w*u).sum() / w.sum()) / w.mean()) >  float(analysisthresholds[analysisIndex][2]):
                avg_ko = True
                kostation = True
            pCounter = 0
            for polarization in analysispolars[analysisIndex]:
                x = xs[polarization]
                y = ys[polarization]
                dmask = dmasks[polarization]
                ndisc = discStations[analysisIndex][stationIndex][polarization]
                stationInfo[stationIndex].append(str(ndisc))
                if avg_ko:
                    (okColor, koColor) = ('k','k')
                elif ndisc > int(float(analysisthresholds[analysisIndex][1]) * len(x)):
                    # Bad polarization
                    (okColor, koColor) = ('m','m')
                    kostation = True
                else:
                    (okColor, koColor) = ('b','r')
                    
                ax = fig.add_subplot(len(analysispolars[analysisIndex]),nx,(sCounter + (nx * pCounter)) +1)
                axes = fig.gca()    
                ax.annotate(ndisc, xy=(.5, .9),  xycoords='axes fraction', horizontalalignment='center', verticalalignment='top',fontsize=10)
                axes.plot(x[~dmask], y[~dmask], okColor+'.',x[dmask], y[dmask], koColor+'.',)
                axes.set_ylim(minStat[analysisIndex], maxStat[analysisIndex])
                # Show X label only in last polarization    
                if pCounter == len(analysispolars[analysisIndex])-1:
                    axes.set_xlabel(station+ ' (' + str(stationIndex) + ')', rotation=45)
                # Show Y label only for first station
                if sCounter == 0:
                    axes.set_ylabel(POL_INDEXES_NAMES[polarization], rotation='horizontal', horizontalalignment='center')
                else:
                    axes.set_yticklabels([])
                
                axes.set_xticklabels([])        
                pCounter += 1
            if kostation:
                analysisBadCounter[stationIndex] += 1
            stationInfo[stationIndex].append(str(kostation))
            sCounter += 1
        fig.subplots_adjust(hspace=0.)
        fig.subplots_adjust(wspace=0.)
    
    # third analysis (no plot here)
    analysisIndex = 2
    sumnumThres = analysisthresholds[analysisIndex][0] 
    sumnumThresAvg = analysisthresholds[analysisIndex][1]
    for stationIndex in stationsIndexes:
        station = dictName[stationIndex]
        (xs, ys,num_mean) = plotInfo[analysisIndex][stationIndex]
        sumnummeanavg = numpy.array(num_mean.values()).mean()
        kostation = False
        for polarization in analysispolars[analysisIndex]:
            ymean = int(num_mean[polarization])
            stationInfo[stationIndex].append(str(ymean))
            if sumnummeanavg < sumnumThresAvg*max_num_mean[polarization]:
                kostation = True
            elif ymean < sumnumThres*max_num_mean[polarization]:
                kostation = True
        if kostation:
                analysisBadCounter[stationIndex] += 1
        stationInfo[stationIndex].append(str(kostation))
        
    lines = []
    headerToPrint = ['#ID','NAME\t']
    for i in range(NUM_ANALYSIS):
        for polar in analysispolars[i]:
            headerToPrint.append(analysisshortname[i][0] + POL_INDEXES_NAMES[polar])
        headerToPrint.append(analysisshortname[i][0] + 'Bad')
    headerToPrint.append('BAD')
    lines.append('\t'.join(headerToPrint))
    for stationIndex in stationsIndexes:
        line = [str(stationIndex), dictName[stationIndex]]
        line.extend(stationInfo[stationIndex])
        # We set a BAD flag if at least two analysis said so
        if analysisBadCounter[stationIndex] > 1:
            line.extend([str(True)])
        else:
            line.extend([str(False)])
        lines.append('\t'.join(line))
        
    if opts.out == '':
        for line in lines:
            print line
        plt.show()
    else:
        for i in (0,1):
            print '-> '+opts.out+'-%s.%s'%(analysisname[i],opts.ext)
            plts[i].savefig(opts.out+'-%s.%s'%(analysisname[i],opts.ext))
        outputfile = open(ofile,'w')
        for line in lines:
            outputfile.write(line+'\n')
        outputfile.close()
        

opt = optparse.OptionParser()
opt.add_option('-i','--input',help='Input stats file',default='')
opt.add_option('-m','--mean',default=False,help='In the median analysis use mean instead? [default False]',action='store_true')
opt.add_option('-q','--mpolar',help='In the median (or mean) analysis: polarizations to use [default is 0,1,2,3]',default='0,1,2,3')
opt.add_option('-c','--mcoord',help='In the median (or mean) analysis: complex coordinate to use (amp,phase,real or imag) [default is amp]',default='amp')
opt.add_option('-t','--mthres',help='In the median (or mean) analysis: The thresholds to use. Provide them comma-separated. First threshold is for candidates, second one is for each polarization and third one is for average of polarizations [default is 3,0.4,0.2]',default='3,0.4,0.2')
opt.add_option('-r','--spolar',help='In the std analysis: polarizations to use [default is 1,2]',default='1,2')
opt.add_option('-s','--scoord',help='In the std analysis: complex coordinate to use (amp,phase,real or imag) [default is real]',default='real')
opt.add_option('-e','--sthres',help='In the std analysis: The thresholds to use. [default is 3,0.4,0.2]',default='3,0.4,0.2')
opt.add_option('-n','--npolar',help='In the num analysis: polarizations to use [default is 0]',default='0')
opt.add_option('-l','--nthres',help='In the num analysis: The thresholds to use. In this case there are only two threshold [default is 0.4,0.3]',default='0.4,0.3')
opt.add_option('-o','--out',default='',help='Output basename [default \'\' means to show the plot/table instead of save it] Filenames will be <basename>-<mean,median,std or num>.<extension> for the plots and <basename>-tab for the table',type='string')
opt.add_option('-x','--ext',default='pdf',help='Image filename extension [default pdf]',type='string')
options, arguments = opt.parse_args()

main(options)
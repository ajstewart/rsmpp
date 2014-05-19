#!/usr/bin/env python
"""
This scripts is used to produce basic but important data statistics from visibility data. 
The visibility data can be a single individual MS or can also use a gds file which has many MSs 
listed inside to produce the statistics for all the files. 

Written by Oscar Martinez

version 0.1 28/03/2012: Initial version
version 0.2 30/03/2012: No error if output exists
                        Added option to overwrite files
version v0.3 02/07/2012: Rename polarization to correlation
"""

################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import sys,numpy,os, optparse
try:
    import pyrap.tables as pt
except ImportError:
        print "Error: The pyrap tables module is not available."
        print "Perhaps you need to first type \'use LofIm\'?"
        print "If you use a build from different day, "
        print "set it as well when running the script with -b option"
        exit()

#
# MS operations
#
def getData(table,column):
    splitColumn = column.split(',')
    if len(splitColumn) == 1:
        return table.getcol(column)
    elif len(splitColumn) == 3:
        columnAData = table.getcol(splitColumn[0])
        operation = splitColumn[1]
        columnBData = table.getcol(splitColumn[2])
        
        if operation == '-':
            return (columnAData-columnBData)
        elif operation == '+':
            return (columnAData+columnBData)
    # If we reach this point it means that the column is not correct
    print 'Column to plot: ' + column + ' is not correct!'
    return None

def getCorrData(data,corrindex):
    if len(data.shape) == 3:
        return data[:, :, corrindex]
    else:
        #some average has been done in time or freq
        return data[:, corrindex]

# This gets the full 3D with all correlations and maybe a cut in channels
# convert to stokes and set the flags if required 
def get3DCutData(table, column, showFlags, flagCol, channels, stokes):
    # Get the data 
    tmp0 = getData(table, column)
    if tmp0 != None:
        if showFlags:
            # If the flag data must be shown we ignore the flag mask
            tmp1 = numpy.ma.array(tmp0, dtype=None, mask=False)
        else:
            tmp1 = numpy.ma.array(tmp0, dtype=None, mask=table.getcol(flagCol))
        # Flag the nan values
        tmp1[numpy.isnan(tmp1)]=numpy.ma.masked
        
        # Convert to stokes
        if stokes:
            tmp2 = numpy.ma.transpose(numpy.ma.array([tmp1[:,:,0]+tmp1[:,:,3],tmp1[:,:,0]-tmp1[:,:,3],tmp1[:,:,1]+tmp1[:,:,2],numpy.complex(0.,-1.)*(tmp1[:,:,2]-tmp1[:,:,1])],dtype=None,mask=tmp1.mask),(1,2,0))
        else:
            tmp2 = tmp1
            
        # this contains the cut in channels desired by the user
        return tmp2[:,channels[0]:channels[1],:]
    
# Integrate the data over certain axis, prefaxis tells us in which axis we are interested (the other will be integrated)
# if prefaxis is None, none axis is integrated 
def getIntegratedData(cutdata, prefaxis=0):
    if cutdata is None:
        print 'Error: selected data'
        return None
    # We average the axis if needed
    if prefaxis == 0:
        return numpy.ma.mean(cutdata,axis=1) # 2D
    elif prefaxis == 1:
        return numpy.ma.mean(cutdata,axis=0) # 2D
    else:
        # No average over axis, only cut in channels 
        return cutdata # 3D     

# Get the complex component of an operation over the correlations (2D or 1D of float numbers)
def getIntegratedDataOperation(cutdata, operation, prefaxis=0):
    intdata = getIntegratedData(cutdata,prefaxis)
    if operation == 1:
        # Special operation : XX - YY
        return getCorrData(intdata, 0) - getCorrData(intdata, 3)
    elif operation == 2:
        # Special operation : XY . YX*
        return getCorrData(intdata, 1) * getCorrData(intdata, 2).conjugate()
    else:
        print 'Error: Requested operation not implemented'
        return None   
    
def getComplexIntCompData(intdata, complexcomp, unwrap):
    if complexcomp == 'real':
        return intdata.real
    elif complexcomp == 'imag':
        return intdata.imag
    elif complexcomp == 'amp':
        return numpy.ma.array(numpy.abs(intdata),mask=intdata.mask)
    elif complexcomp.count('phase'):
        phaseop = numpy.angle(intdata)
        if complexcomp == 'phase':
            if unwrap:
                phaseop = numpy.unwrap(phaseop,axis=0)
            return numpy.ma.array(phaseop, mask=intdata.mask)
        else:
            # phase_rate
            phaseop = numpy.unwrap(phaseop,axis=0)
            tmp=numpy.diff(phaseop,axis=0)
            return numpy.ma.array(numpy.append(tmp,numpy.array([tmp[-1],]),axis=0), mask=intdata.mask)
    
def getStats(intData, compCoordDict, statparams, doUnwrap):
    statresults = []
    for i in range(len(statparams)):
        statresults.append(float('nan'))
    if intData.count():
        for complexcomponent in compCoordDict:
            complexIntCompData = getComplexIntCompData(intData, complexcomponent, doUnwrap)
            indexes = compCoordDict[complexcomponent]
            for index in indexes:
                statistic = statparams[index]
                if statistic == 'mean':
                    statresults[index] = complexIntCompData.mean()
                elif statistic == 'median':
                    statresults[index] = numpy.ma.median(complexIntCompData)
                elif statistic == 'std':
                    statresults[index] = complexIntCompData.std()
    return statresults

#
# UTILS METHODS
#

# Get from a GDS file a list of absPaths and nodes with the MSs
def gdsToPathNode(gdsFile):
    if not os.path.isfile(input):
        raise Exception('Error: ' + input + ' does not exists')
    
    gdsfile = open(input, 'r')
    gdslines = gdsfile.read().split('\n')
    gdsfile.close()
    absPaths = []
    nodes = []
    
    for line in gdslines:
        if line.count('FileName'):
            try:
                absPaths.append(line.split('=')[-1].strip())
            except:
                continue
        elif line.count('FileSys'):
            try:
                nodes.append(line.split('=')[-1].split(':')[0].strip())
            except:
                continue
    
    if len(absPaths) != 0 and len(nodes) == 0:
        cnode = getHostName()
        for i in range(len(absPaths)):
            nodes.append(cnode)
            
    if len(absPaths) != len(nodes):
        raise Exception('Error: reading GDS file')
    
    return (absPaths, nodes)

# Get the current node name
def getHostName():
    return (os.popen("'hostname'")).read().split('\n')[0]

# Parse a string into a boolena
def booleanStringToBoolean(booleanString):
    if booleanString == 'True':
        return True
    else:
        return False
    
# Parse from a list of strings to a single string with them comma-separated
def listToString(elements, separation = ',', widths = None):
    commaSeparatedString = ''
    for i in range(len(elements)):
        element = str(elements[i])
        if widths != None:
            if widths[i] != None:
                for i in range(widths[i] - len(element)):
                    commaSeparatedString += ' '
        commaSeparatedString += element + separation
    return commaSeparatedString[:-1]
    
# elements is a list of objects. These objects may be duplicated. This method 
# returns a dictionary: the keys are the different elements (without
# duplications). Each value will be a list of the indexes in the original 
# elements array
def getIndexesDictionary(elements):
    
    indexesPerElement = {}
    
    for i in range(len(elements)):
        if elements[i] in indexesPerElement.keys():
            indexes = indexesPerElement[elements[i]]
        else:
            indexes = []
        
        indexes.append(i)
        indexesPerElement[elements[i]] = indexes
        
    return indexesPerElement

# Split an array in a list of arrays. The way the partition is done guarantees
# that the sizes of the subarrays may vary +-1
# For example if we have an array with 11 elements and we want 3 subarrays the 
# sizes of them would be 4,4,3 instead of 5,5,1
def splitArray(arrayToSplit, numSubArrays):
    quo = len(arrayToSplit) / numSubArrays
    res = len(arrayToSplit) % numSubArrays
    
    indexInArray = 0
    
    subarrays = []
    for i in range(numSubArrays):
        if i < res:
            numElements=quo+1
        else:
            numElements=quo
        
        subarray = []
        for j in range(numElements):
            subarray.append(arrayToSplit[indexInArray])
            indexInArray = indexInArray + 1
            
        subarrays.append(subarray)
        
    return subarrays

# Split an dictionary in a list of dictionary. The way the partition is done guarantees
# that the sizes of the subdictionary may vary +-1
# For example if we have an dictionary with 11 elements and we want 3 subdictionary the 
# sizes of them would be 4,4,3 instead of 5,5,1
def splitDictionary(dictionaryToSplit, numSubDictionaries):
    quo = len(dictionaryToSplit) / numSubDictionaries
    res = len(dictionaryToSplit) % numSubDictionaries
    
    indexInDictionaryValuesArray = 0
    
    subdictionaries = []
    for i in range(numSubDictionaries):
        if i < res:
            numElements=quo+1
        else:
            numElements=quo
        
        subdictionary = {}
        for j in range(numElements):
            key = dictionaryToSplit.keys()[indexInDictionaryValuesArray]
            subdictionary[key] = dictionaryToSplit[key]
            indexInDictionaryValuesArray = indexInDictionaryValuesArray + 1
            
        subdictionaries.append(subdictionary)
        
    return subdictionaries


# This python module contains code to distribute tasks over several children.
# Some children may have different whats assigned to it. User can select how 
# many simultaneous processes run each children and how many simultaneous 
# children are running processes.
# 
# What is to be done is decided with a function passed as argument
# This function will receive a childid and a what and will return a string
# In the end we collect all the results of the processing of the several pairs
# and we return all them in a string 

# The process method arguments are:
#    childrenIds list of strings with childrenIds
#    whats list of strings with instructions to be used for the children
#    functionToApply function that each children will apply to its related whats
#              it will return a string
#    maximumProcessorsToUse maximum processes being run by a single children
#    maximumSimultaneousChildren maximum number of children running processes
def processdistribute(childrenIds, whats, functionToApply, maximumProcessorsToUse, maximumSimultaneousChildren):

    # Flushed the current buffered data in sys.stdout
    sys.stdout.flush()   

    # We get the different involved children. Each child has a list of the indexes
    sbsIndexesPerChild = getIndexesDictionary(childrenIds)
    
    simultChildrenDictionariesList = splitDictionary(sbsIndexesPerChild, maximumSimultaneousChildren)
    
    readPipes = {}
    writePipes = {}
    # We open the pipes -> a pipe for each whats
    for index in range(len(whats)):
        readPipes[index], writePipes[index] = os.pipe()
    
    pid={}
    
    for simultChildSequenceIndex in range(len(simultChildrenDictionariesList)):
        
        simultChildDictionary = simultChildrenDictionariesList[simultChildSequenceIndex]
    
        pid[simultChildSequenceIndex] = os.fork()
        
        if not pid[simultChildSequenceIndex]:
            
            # we are the child 
            pidc = {}
            
            for child in simultChildDictionary:
            
                sbsIndexes = simultChildDictionary[child]
                sbsIndexesSubArrays = splitArray(sbsIndexes, maximumProcessorsToUse)
                
                for j in range(len(sbsIndexesSubArrays)):
                    
                    pipeKey = child + str(j)
                    pidc[pipeKey] = os.fork()
                
                    sbsIndexesSubArray = sbsIndexesSubArrays[j]
                    
                    if not pidc[pipeKey]:
                        # we are the grandchild
                        for index in sbsIndexesSubArray:

                            result = functionToApply(child, whats[index])
                            os.close(readPipes[index])
                            writePipes[index] = os.fdopen(writePipes[index], 'w')
                            writePipes[index].write(result)
                            writePipes[index].close()
                        sys.exit(0)
                
                for j in range(len(sbsIndexesSubArrays)):   
                    pipeKey = child + str(j)
                    os.waitpid(pidc[pipeKey],0) # make sure the grandchild process gets cleaned up

                for j in range(len(sbsIndexesSubArrays)):
                    for index in sbsIndexesSubArrays[j]:
                        os.close(writePipes[index])
                        os.close(readPipes[index])
            sys.exit(0)
     
    result=''
    for simultChildSequenceIndex in range(len(simultChildrenDictionariesList)):
        os.waitpid(pid[simultChildSequenceIndex],0) # make sure the child process gets cleaned up
        
    
    for index in range(len(whats)):
        os.close(writePipes[index])
        aux = os.fdopen(readPipes[index])
        towrite = aux.read()
        result = result + towrite
        aux.close()
    return result


def processMS(absPath, output,overwrite,stats,column,timeslots,channels,antennas, correlations, wrap, flag, colflag, stokes, autocorr,operation, acc):
    overWrite = booleanStringToBoolean(overwrite)
    showFlags = booleanStringToBoolean(flag)
    acc = int(acc)
    showAutocorr = booleanStringToBoolean(autocorr)
    flagCol = colflag
    timeslots = timeslots.split(',')
    if len(timeslots) != 2:
        print 'Error: Timeslots format is start,end'
        return
    for i in range(len(timeslots)): timeslots[i] = int(timeslots[i])
    antToPlotSpl = antennas.split(',')
    antToPlot = []
    for i in range(len(antToPlotSpl)):
        tmpspl = antToPlotSpl[i].split('..')
        if len(tmpspl) == 1:
            antToPlot.append(int(antToPlotSpl[i]))
        elif len(tmpspl) == 2:
            for j in range(int(tmpspl[0]),int(tmpspl[1])+1):
                antToPlot.append(j)
        else:
            print 'Error: Could not understand antenna list.'
            return
    corrs = correlations.split(',')
    for i in range(len(corrs)):
        corrs[i] = int(corrs[i])
    
    convertStokes = booleanStringToBoolean(stokes)  
    if operation != '':
        operation = int(operation)
        if convertStokes:
            print 'Error: Stokes conversion is not compatible with special operations'
            return
    
    channels = channels.split(',')
    if len(channels) != 2:
        print 'Error: Channels format is start,end'
        return
    for i in range(len(channels)): channels[i] = int(channels[i])
    if channels[1] == -1:
        channels[1] = None # last element even if there is only one
    else:
        channels[1] += 1
    doUnwrap = booleanStringToBoolean(wrap)

    # open the main table and print some info about the MS
    t = pt.table(absPath, readonly=True, ack=False)
    
    # we get the first and last time, assuming the data is sorted by TIME
    times = pt.taql('select DISTINCT TIME from ' + absPath)
    if timeslots[1] == -1:
        timeslots[1] = times.nrows() -1
    
    for i in range(len(timeslots)):
        if (timeslots[i] < 0) or (timeslots[i] > len(times)):
            print 'Error: specified timeslots out of valid range, number samples is ' + str(len(times))
            return
    
    # Station names
    antList = pt.table(t.getkeyword('ANTENNA'), readonly=True, ack=False).getcol('NAME')
    if len(antToPlot)==1 and antToPlot[0]==-1:
        antToPlot = range(len(antList))

    freq = pt.table(t.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False).getcell('REF_FREQUENCY',0)/1.e6
    
    statslist = stats.split(',')
    complexcoordinates = []
    statparams = []
    for stat in statslist:
        if stat.count('_'):
            sfields = stat.split('_')
            if len(sfields) == 2:
                complexcoordinates.append(sfields[0])
                statparams.append(sfields[1])
    if len(complexcoordinates) == 0:
        print 'Error: check specified stats format'
        return
    for complexcoord in complexcoordinates:
        if complexcoord not in ('amp','phase','real','imag','phaserate'):
            print 'Error: check specified stats format'
            return
    for statparam in statparams:
        if statparam not in ('mean','median','std'):
            print 'Error: check specified stats format'
            return
    compCoordDict = getIndexesDictionary(complexcoordinates)
    
    header = ['#sbfreq','ant1', 'ant2', 'ant1Name', 'ant2Name', 'corr', 'num', 'numflag']
    colwidths = [9,5,5,15,15,4,10,10]
    header.extend(statslist)
    for i in range(len(statslist)):
        colwidths.append(8+acc)
    os.system('mkdir -p ' + output)
    ofilename = output + '/' + absPath.split('/')[-1] + '.stats'
    if os.path.isfile(ofilename):
        if overWrite:
            os.system('rm ' + ofilename)
        else:
            print 'Error: ' + ofilename + ' already exists! (maybe you want to use option -d)'
            return
    outputfile = open(ofilename, "w")
    
    outputfile.write(listToString(header, '\t', widths = colwidths) + '\n')
    
    lines = []
    # select by time from the beginning, and only use specified antennas
    tsel = t.query('TIME >= %f AND TIME <= %f AND ANTENNA1 IN %s AND ANTENNA2 IN %s' % (times.getcell('TIME',timeslots[0]),times.getcell('TIME',timeslots[1]),str(antToPlot),str(antToPlot)))

    if convertStokes:
        corrLabels = ['I','Q','U','V']
    else:
        corrLabels = ['XX','XY','YX','YY']
    
    # prefaxis is to make samples integrated over the f axis after the cut
    prefaxis = 0

    # Now we loop through the baselines
    for tpart in tsel.iter(["ANTENNA1","ANTENNA2"]):
        ant1 = tpart.getcell("ANTENNA1", 0)
        ant1Name = antList[ant1]
        ant2 = tpart.getcell("ANTENNA2", 0)
        ant2Name = antList[ant2]
        if ant1 not in antToPlot or ant2 not in antToPlot: continue
        if ant1 == ant2:
            if not showAutocorr:
                continue
        
        # Get the 3D cut data [time][freq][corr]
        cutData = get3DCutData(tpart, column, showFlags, flagCol, channels, convertStokes)
        
        if cutData is None: # This baseline must be empty, go to next one
            print 'No good data on baseline %s - %s' % (ant1Name,ant2Name)
            continue
        
        if operation != 0: # A special operation of the correlations is required
            
            # we get the integrated data 
            intData = getIntegratedDataOperation(cutData, operation, prefaxis)
            statsresults = getStats(intData, compCoordDict, statparams, doUnwrap)
                
            # To know the num and nummasked we do not need the operation itself,
            # only to know how the combination of the correlations affected the
            # masks (but we need to do this using the non integrated data)
            if operation == 1:
                label = 'XX-YY'
                cData = getCorrData(cutData, 0) + getCorrData(cutData, 3)
            elif operation == 2:
                label = 'XY.YX*'
                cData = getCorrData(cutData, 1) + getCorrData(cutData, 2)
            num = cData.count()
            nummasked = numpy.ma.count_masked(cData)

            addInfo(lines, ant1,ant2,ant1Name,ant2Name,statsresults, num, nummasked , label, freq, acc, colwidths)
        else:
            
            intData = getIntegratedData(cutData, prefaxis)
            for j in corrs:
                # For each correlations
                # From the integrated array we get mean and std of the desired complex component of each correlations
                statsresults = getStats(getCorrData(intData, j), compCoordDict, statparams, doUnwrap)               
                # To know the num and nummasked we have to use the cut data (before the complex component selection and axis integration)
                corrCutData = getCorrData(cutData, j)
                num = corrCutData.count()
                nummasked = numpy.ma.count_masked(corrCutData)
                
                addInfo(lines, ant1,ant2,ant1Name,ant2Name, statsresults, num, nummasked, corrLabels[j], freq, acc, colwidths)
    tow = ''
    for line in lines:
        tow += line + '\n'
    outputfile.write(tow)
    outputfile.close()
    print getHostName() + ' ' + absPath + ' collecting complete!'
    return

# Add information, i.e. the label inte plot and the statistics if required
def addInfo(lines, ant1,ant2,ant1Name,ant2Name, stats, num, nummasked, label, freq, acc, colwidths):
    form = '%.'+str(acc) + 'e'
    row = [("%.3f" % freq),('%d'%ant1),('%d'%ant2),ant1Name,ant2Name,label,('%d'%num),('%d'%nummasked)]
    for i in range(len(stats)):
        row.append(form % stats[i])
    lines.append( listToString(row, '\t', widths = colwidths))

        
# Function used for the tasksdistributor
def function(node, what):
    (absPath, output, overwrite, stats,column,timeslots,channels,antennas,correlations, wrap, flag, colflag, stokes, autocorr,operation,acc,build) = what
    scriptpath  = os.path.abspath(__file__)
    parentpath = os.path.abspath(os.path.join(scriptpath, '..'))
    scriptname = scriptpath.split('/')[-1].split('.')[0]
    command = 'python -c "import ' + scriptname + '; ' + scriptname + '.' + processMS.__name__ + '(\\\"' + absPath +'\\\",\\\"' + str(output) +'\\\",\\\"' + str(overwrite) +'\\\",\\\"' + str(stats) +'\\\",\\\"' + str(column) +'\\\",\\\"' + str(timeslots)  +'\\\",\\\"' + str(channels)  +'\\\",\\\"' + str(antennas)  +'\\\",\\\"' + str(correlations)  +'\\\",\\\"' + str(wrap)  +'\\\",\\\"' + str(flag)   +'\\\",\\\"' + str(colflag)   +'\\\",\\\"' + str(stokes)   +'\\\",\\\"' + str(autocorr)   +'\\\",\\\"' + str(operation) +'\\\",\\\"' + str(acc) +'\\\")"'
    
    if node == getHostName():
        return (os.popen("cd " + parentpath + " ; " + command)).read()
    else:
        if build != '':
            build = 'use LofIm ' + build + ';'
        return (os.popen("ssh " + node + " 'cd " + parentpath + " ; " + build+ command + "'")).read()
    
def main(opts):
    # Check the aguments
    input = opts.input
    if input != '':
        if not os.path.isfile(input) and not os.path.isdir(input):
            print 'Error: ' + input + ' does not exist'
            exit()
    else:
        print 'Error: No input specified!'
        exit()
    input = os.path.abspath(input)
    
    output = opts.output
    if output == '':
        print 'Error: No output specified!'
        exit()
    output = os.path.abspath(output)
    if input.endswith('gds') or input.endswith('GDS'):
        print 'GDS as input is untested.'
        (absPaths,nodes) = gdsToPathNode(input)
    else:
        # We assume single MS
        (absPaths,nodes) = ([input,],[getHostName(),])
    
    if not len(absPaths):
        print "No MSs to be processed!"
        return
    
    whats = []
    for absPath in absPaths:
        whats.append((absPath, output, opts.overwrite, opts.stats,opts.column,opts.timeslots,opts.channels,opts.antennas,opts.correlations, opts.wrap, opts.flag, opts.colflag, opts.stokes, opts.autocorr,opts.operation,opts.acc,opts.build))
    
    if len(absPaths) > 1:
        print 'Collecting in the nodes...'
    result = processdistribute(nodes, whats, function, int(opts.numprocessors), int(opts.numnodes))
    rsplit = result.split('\n')
    for mes in rsplit:
        if mes != '':
            print mes
            
    globaloutput = output + '/GLOBAL_STATS'
    if os.path.isfile(globaloutput):
        os.system('rm ' + globaloutput)
    ndir = len(os.listdir(output))
    if ndir > 1:
        print 'Collecting finished. Joining results in ' + globaloutput
        os.system('cat ' + output + '/* > ' + globaloutput)
    elif ndir == 1:
        print 'Collecting finished. Check results in ' + output
    else:
        print 'None statistic files have been generated'
            

if __name__ == "__main__":
    
    version_string = 'v0.3, 02 July 2012\nWritten by Oscar Martinez'
    print 'asciistats.py',version_string
    print ''
    
    opt = optparse.OptionParser()
    opt.add_option('-i','--input',help='MS path/GDS file',default='')
    opt.add_option('-r','--output',help='Output stats folder',default='')
    opt.add_option('-d','--overwrite',default=False,help='Overwrite files in output folder if already there?  [default False]',action='store_true')
    opt.add_option('-y','--stats',help='Stats to output. The format for each stat is [complex coordinate]_[statistic]. The options for complex coordinate: amp|phase|real|imag|phaserate and for statistic: mean|median|std. Multiple values are possible [default is amp_median,real_std]',default='amp_median,real_std')
    opt.add_option('-c','--column',help='Column to use. The options are DATA|CORRECTED_DATA|MODEL_DATA but also other columns that the user may have created. It is also possible to specify a combination of two columns. For example the user may want the CORRECTED_DATA-MODEL_DATA. In such example the user should write: CORRECTED_DATA,-,MODEL_DATA. Currently + and - are supported. [defaul is DATA]',default='DATA')
    opt.add_option('-t','--timeslots',help='Timeslots to use (comma separated and zero-based: start,end[inclusive]). Negative values work like python slicing, but please note that the second index here is inclusive [default is 0,-1].',default='0,-1')
    opt.add_option('-s','--channels',help='Channels to use (comma separated and zero-based: start,end[inclusive]). Negative values work like python slicing, but please note that the second index here is inclusive [default is 0,-1].',default='0,-1')
    opt.add_option('-e','--antennas',help= 'Antennas to use (comma separated list, zero-based) To specify an inclusive range of antennas use .. format, e.g. -e 0..9 requests the first 10 antennas. To see which antennas are available use uvplot -q with some of the ms',default='-1')
    opt.add_option('-p','--correlations',help='Correlations to use (it does not convert, so use integers as in the MS) [default is 0,1,2,3].',default='0,1,2,3')
    opt.add_option('-w','--wrap',default=False,help='Unwrap phase? [default False]',action='store_true')
    opt.add_option('-f','--flag',default=False,help='Show flagged data? [default False]',action='store_true')
    opt.add_option('-g','--colflag',help='Column that contains flags [default is FLAG]',default='FLAG')
    opt.add_option('-k','--stokes',default=False,help='Convert to Stokes IQUV?',action='store_true')
    opt.add_option('-u','--autocorr',default=False,help='Show autocorrelations?, this refers to baseline autocorrelations',action='store_true')
    opt.add_option('-o','--operation',help='Use an special operation over the correlations. (choose from 0|1|2). 0 is for none operation (normal correlations are used), 1 is XX-YY and 2 is XY.YX*. If some operation is specified the options correlations and stokes are ignored. [default is 0, i.e. none operation]',default='0')
    opt.add_option('-a','--acc',help='Accuracy in the given statistics [default is 4]',default='4')
    opt.add_option('-j','--numprocessors',help='Simultaneous processes, only applying if GDS file is given [default is 1]',default='1')
    opt.add_option('-n','--numnodes',help='Simultaneous nodes, only applying if GDS file is given [default is 64]',default='64')
    opt.add_option('-b','--build',help='Use different build day, only applying if GDS file is given. Only provide this if you had to do use LofIm XXX (the options are Mon,Tue,Wed,Thu,Fri,Sat and Sun) [default is to use current day]',default='')
    options, arguments = opt.parse_args()
    main(options)
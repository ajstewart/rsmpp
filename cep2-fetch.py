#!/usr/bin/env python

# To use this script please set your .ssh/config file accordingly e.g.
# Host lofar
#   HostName portal.lofar.eu
#
# Host lhn001
#   ServerAliveInterval 30
#   ProxyCommand ssh <user>@lofar netcat -w90 %h %p
#
# Host locus*
#   ServerAliveInterval 30
#   ProxyCommand ssh <user>@lhn001 netcat -w90 %h %p
#
# And make sure your ssh keys are set up for passwordless login!

import subprocess
import optparse, os
from multiprocessing import Pool
from functools import partial
import getpass
from itertools import cycle
import time
import sys

def fetch(node, username, dir, obsid, files):
    command="rsync -qru {0}@locus{1:03d}:{2} .".format(username, node, os.path.join(dir, obsid, files))
    try:
        subprocess.call(command, shell=True)
    except:
        print "No files found on locus{:03d}\n".format(node)
    return

usage = "usage: python %prog [options] html.txt"
description="Script to quickly fetch data from CEP2.\n\
To use this script please set your .ssh/config file accordingly e.g.\n\
Host lofar\n\
  HostName portal.lofar.eu\n\
\n\
Host lhn001\n\
  ServerAliveInterval 30\n\
  ProxyCommand ssh <user>@lofar netcat -w90 %h %p\n\
\n\
Host locus*\n\
  ServerAliveInterval 30\n\
  ProxyCommand ssh <user>@lhn001 netcat -w90 %h %p\n\
\n\
And make sure your ssh keys are set up for password-less login to CEP2!"
vers="1.0"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-d", "--cepdirectory", action="store", type="string", dest="cepdir", default="/data/", help="Directory  on locus nodes [default: %default].")
parser.add_option("-g", "--filesglob", action="store", type="string", dest="glob", default="*.dppp.MS", help="Glob pattern of files to fetch [default: %default].")
parser.add_option("-l", "--locusnodes", action="store", type="string", dest="nodes", default="1-100", help="Range of locus nodes to check [default: %default].")
parser.add_option("-s", "--obsidstep", action="store", type="int", dest="step", default=2, help="Step value of observation ids [default: %default].")
parser.add_option("-u", "--cepuser", action="store", type="string", dest="user", default=getpass.getuser(), help="Step value of observation ids [default: %default].")
(options, args) = parser.parse_args()

start=int(args[0].replace("L", ""))
end=int(args[1].replace("L", ""))
step=options.step

obsids=range(start, end+step, step)
obsids=["L{}".format(j) for j in obsids]

tempsplit=options.nodes.split("-")
nodes=range(int(tempsplit[0]), int(tempsplit[1])+1)

print "Will look for files on locus nodes {0:03d} - {1:03d}".format(nodes[0], nodes[-1])

print "Belonging to Observations IDs:"
print " ".join(obsids)

cepdir=options.cepdir
cepuser=options.user
print "In location: {}".format(cepdir)

print "----------------------------------------------------------------"
# time.sleep(1)

fetchworkers=Pool(processes=2)
spinner = cycle(['-', '/', '|', '\\'])

for i in obsids:
    fetch_multi=partial(fetch, username=cepuser, dir=cepdir, obsid=i, files=options.glob)
    print "Fetching {} files...".format(i)
    done=fetchworkers.map_async(fetch_multi, nodes)
    while not done.ready():
        sys.stdout.write(spinner.next())
        sys.stdout.flush()
        time.sleep(0.1)
        sys.stdout.write('\b')
    print "{} done!".format(i)

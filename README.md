rsmpp-master
============

Radio Sky Monitor Processing Pipeline v2.X

============

Python Version 2.7.3

Required modules (should all be standard except pyrap):

* subprocess
* multiprocessing
* os
* glob
* optparse
* sys
* datetime
* string
* getpass
* time
* logging
* ConfigParser
* functools
* itertools
* numpy
* pyrap
* base64

LOFAR Imaging Tools also required. (LofIm tools)

============

Standalone product - no installation really required. Just clone the entire directory and scripts will be ready to run. Just add the directory to a path using something like:

\#!/usr/bin/env sh

PATH="/path/to/rsmpp/rsmpp-master:${PATH}"
export PATH

If you would like to use the email function please contact me to obtain the pipeline email account password.

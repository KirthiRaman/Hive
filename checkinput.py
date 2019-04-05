#!/usr/bin/env python
import gzip
import csv

import json, re
import datetime as dt
import subprocess
import os, sys, time
import os.path

def runCommand(command):
  try:
    mypipe=subprocess.Popen(command, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=sys.stderr)
    mypipe.wait()
  except:
      print("Warning: While excuting " + command + "\n")

  mystdout, mystderr = mypipe.communicate()
  returncode = mypipe.returncode

  return (mystdout, mystderr, returncode)

def checkContents(cmd):
   jobstdout, jobstderr, returncode = runCommand(cmd)
   return returncode

inpfname=""
fn = raw_input("What is the Input Filename for this new ticket? ")
print fn
if ".gz" in fn:
  if "/FOR_MOBILE" in fn:
       inpfname = fn
  else:
       inpfname = "/mnt/SecureCluster/user/o2oetl/FOR_MOBILE/"+fn
else:
  if "/FOR_MOBILE" in fn:
       inpfname = fn+".gz"
  else:
       inpfname = "/mnt/SecureCluster/user/o2oetl/FOR_MOBILE/"+fn+".gz"

print inpfname
retcode = checkContents("ls -lrt "+inpfname)
if retcode != 0:
  print inpfname+" does not exist"
  checkContents("rm -r /tmp/123.txt")
else:
  checkContents("gzip -cd "+inpfname+" | head > /tmp/123.txt")
  fname = "/tmp/123.txt"
  with open(fname, 'r') as fin:
    print fin.read()
  
cdmtitkt = raw_input("Which ticket? ")
cdmtitkt = re.sub(r'\W+', '', cdmtitkt)
print "You may run the following command now"
print "python experian_auto.py "+inpfname+" "+cdmtitkt + " &"

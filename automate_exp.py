#!/usr/bin/env python
import gzip
import csv

import json, re
import datetime as dt
import subprocess
import os, sys, time
import os.path

from stat import S_ISREG, ST_CTIME, ST_MODE, ST_MTIME

inpscripts  = [ "00_create_experian_tables.hql", "00_create_experian_tables2.hql", "00_create_experian_tabsep_tables.hql", 
                "00_create_experian_tabsep_tables2.hql"]
stepscripts = [ "dummy", "1_create_luid_pid.hql", "2_create_epid_email_pid.hql", "3_experian_results.hql", "4_stats.hql"]
experian_hdfs=" /mnt/SecureCluster/nis/prod/data/mobiletl/testfeeds/experian/"
workarea = "/opt/mobiletl/cdmti/Experian_Individual_Scripts/o2o/"

fname = sys.argv[1]
jname = sys.argv[2]
def lastStepCompleted():
     #Completed Step 1
     file = open("/tmp/"+jname+"_completion.txt", 'r')
     lint =0
     thisstep=0
     try:
       for line in file:
         print line
         cols = line.split(" ")
         thisstep=int(cols[2])
         if thisstep > lint: lint=thisstep
     except:
        lint = lint
     return lint

def showFinalStat(jname,distdid,distclid):
     csvread = open(workarea+"stats/"+jname+"_raw.txt", 'r')
     csread = csv.reader(csvread, delimiter='\t')
     pidct=luidct=epidct=0
     for row in csread:
       #print row[0], row[0].find("experian_")
       if row[0].find("experian_") != -1:
         if row[0].find("pid") != -1:
            pidv=1
            luidv=epidv=0
         elif row[0].find("luid") != -1:
            luidv=1
            pidv=epidv=0
         elif row[0].find("person_id") != -1:
            epidv=1
            pidv=luidv=0
       else:
         if pidv == 1:
            pidct=int(row[0])
         elif luidv == 1:
            luidct=int(row[0])
         elif epidv == 1:
            epidct=int(row[0])
     #print "PID ",pidct, " LUID ",luidct, " EPID ",epidct
     csvread.close()
     pidavg = float(pidct)*100.0/float(distdid)
     pidavg = float("{0:.2f}".format(pidavg))
     luidavg = float(luidct)*100.0/float(distclid)
     luidavg = float("{0:.2f}".format(luidavg))
     epidavg = float(epidct)*100.0/float(distclid)
     epidavg = float("{0:.2f}".format(epidavg))
     statw = open(workarea+"stats/"+jname+"_res.txt", 'w')
     statw.write("Extracted PIDS and LUIDS:\n")
     statw.write("\n")
     statw.write("Here are the stats:\n")
     statw.write("Distinct DIDs: "+str(intWithCommas(distdid))+"\n")
     statw.write("Distinct DIDs with Neustar-PID: "+str(intWithCommas(pidct))+" ({color:#d04437}"+str(pidavg)+"%{color})\n")
     statw.write("\n")
     statw.write("Total Client IDs: "+str(intWithCommas(distclid))+"\n")
     statw.write("Distinct client_id with PersonID: "+str(intWithCommas(epidct))+" ({color:#d04437}"+str(epidavg)+"%{color})\n")
     statw.write("Distinct client_id with LUID: "+str(intWithCommas(luidct))+" ({color:#d04437}"+str(luidavg)+"%{color})\n")
     statw.close()

def printMsg(msg):
    with open(workarea+"logs/"+jname+".txt", "a") as jobsfile:
        jobsfile.write(str(msg))
        jobsfile.write("\n")

def runCommand(command):
  if command.find("Completed") > -1 and command.find("logs") > -1  and command.find("tmp") > -1:
    ignore=1
  else:
    printMsg("Debug: About to execute " + command + "\n")
  try:
    mypipe=subprocess.Popen(command, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=sys.stderr)
    mypipe.wait()
  except:
    if command.find("Completed") > -1 and command.find("logs") > -1 and command.find("tmp") > -1:
      ignore=1
    else:
      printMsg("Warning: While excuting " + command + "\n")

  mystdout, mystderr = mypipe.communicate()
  returncode = mypipe.returncode

  return (mystdout, mystderr, returncode)

def intWithCommas(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)

def showStats(jname):
     # was 2.txt
     statmsg = ""
     csvread = open(workarea+"results/"+jname+"_distcounts.txt", 'r')
     csread = csv.reader(csvread, delimiter='\t')
     hasres=0
     rowct=0
     for row in csread:
       print row
       if len(row)> 1 and row[0] == '_c0' and row[1] == '_c1':
          hasres=1
       rowct = rowct+1
       if rowct == 2:
         try:
           distdid = v1 = int(row[0])-1
           distclid = v2 = int(row[1])-1
           statmsg = "There are "+str(intWithCommas(v1))+" distinct device_id and "+str(intWithCommas(v2))+" distinct client_id"
         except:
           print "Couldnt get stats"
     csvread.close()
     #printMsg(statmsg)
     runCommand("echo "+statmsg+" >> /tmp/"+jname+".txt")
     return distdid,distclid

def readInp(sep):
   # was 2.txt
   l=0
   try:
     with open("/tmp/"+jname+".txt", 'r') as csvread:
       csread = csv.reader(csvread, delimiter=sep)
       for row in csread:
         l = len(row)
         return row[0],row[1],l
   except:
      l=0
      return "","",l 

def copyInpFile(inp, jobname):
   #jobstdout, jobstderr, returncode = runCommand("mkdir "+experian_hdfs+jobname)
   returncode=0
   if returncode == 0:
     jobstdout, jobstderr, returncode = runCommand("cp "+inp+experian_hdfs+jobname)
     if jobstderr != None and returncode != 0:
       printMsg("Copy Input File "+inp+" Failed at "+str(dt.datetime.now()))
     return returncode
   else:
     printMsg("Copy Input File "+inp+" Failed at "+str(dt.datetime.now()))
     return returncode

def checkHeader(cmd):
   jobstdout, jobstderr, returncode = runCommand(cmd)
   return returncode

def getContent(cmd, err):
   jobstdout, jobstderr, returncode = runCommand(cmd)

   if jobstderr != None and returncode != 0:
     printMsg(err)

   return returncode

def runSingleCmd(jobname):
   prefcmd = " -d drop_dt=\""+jobname+"\" "
   cmd = "hive -e \"select count(distinct device_id),count(distinct client_id) from experian_external_"+jobname+ "\" | grep -v \"WARN\" > "+workarea+"results/"+jobname+"_distco
unts.txt"

   printMsg(cmd)
   jobstdout, jobstderr, returncode = runCommand(cmd)
   if jobstderr != None and returncode != 0:
      printMsg("Counting Distinct DeviceID,ClientID Failed at "+str(dt.datetime.now()))

   return returncode

def buildStep(ind, jobname, fname):
   prefcmd = " -d drop_dt=\""+jobname+"\" "
   postcmd = " 1> "+workarea+"runs/"+jobname+"_step"+str(ind)+".1  2> "+workarea+"runs/"+jobname+"_step"+str(ind)+".2"
   stepind =int(ind)
   if stepind == 0:
     cmd = "hive" +prefcmd + "-f "+fname + postcmd
   elif stepind != 5:
     cmd = "hive" +prefcmd + "-f "+stepscripts[stepind] + postcmd
   else:
     cmd = "hive -e \"select distinct client_id,luid,person_id from "+jobname+"_experian_full where (luid is not null and luid != '') OR (person_id is not null and person_id !=
 '')\" | grep -v \"WARN\" | sed 's/[\t]/,/g' | sed 's/NULL/''/g' > "+workarea+"results/"+fname+"_results_LUID_EPID.txt"
   printMsg(cmd)
   bstep = int(ind)  # save the step value
   jobstdout, jobstderr, returncode = runCommand(cmd)

   if jobstderr != None and returncode != 0:
     printMsg("Step-"+ind+" Failed at "+str(dt.datetime.now()))

   return returncode

def copyInputFileBySepar(separ):
      print "In copyInputFileBySepar"
      pos = fname.rfind("/")
      resfname = fname[pos+1:]
      pos = resfname.find(".")
      resfname_prefix = resfname[0:pos]
      rcode=1
      if separ == '|':
        col1script = inpscripts[0]
        col2script = inpscripts[1]
      else:
        col1script = inpscripts[2]
        col2script = inpscripts[3]
      printMsg("col1script = "+col1script)
      printMsg("col2script = "+col2script)
      col1,col2, exp1 = readInp(separ)
      if int(exp1) == 3:
        if os.access(fname,os.R_OK) == True:
          rcode = copyInpFile(fname,jname)
          if rcode == 0:
            if col1== "MOBILE_AD_ID" or col1=="DEVICE_ID":
              rcode=buildStep(0,jname,col1script)
            elif col2 == "MOBILE_AD_ID" or col2=="DEVICE_ID":
              rcode=buildStep(0,jname, col2script)
      return rcode, resfname_prefix

def process_steps(resfname_prefix):
    jtdout,jstderr,returncode = runCommand("grep \"Completed Step\" "+workarea+"logs/"+jname+".txt | grep -v \"logs\" > /tmp/"+jname+"_completion.txt")
    print "ResFname_Prefix",resfname_prefix
    print "Returncode = ",returncode
    rcode=1
    startStep = lastStepCompleted()
    startStep = startStep + 1
    printMsg("Step Count "+str(startStep))
    rcode=0  
    if rcode==0:
      # This can be re-run anyways
      #if os.path.exists(workarea+"results/"+jname+"_distcounts.txt") == False:
      #  runSingleCmd(jname)
      runSingleCmd(jname)
      if startStep==1:
        printMsg("Skipping first Step")
        #rcode=buildStep(1,jname,"")
    if rcode==0:
       if startStep <= 2:
         printMsg("Completed Step 1")
         printMsg("Skipping second Step")
         #rcode=buildStep(2,jname,"")
    if rcode==0:
       if startStep <= 3:
         printMsg("Completed Step 2")
         rcode=buildStep(3,jname,"")
    if rcode == 0:
       if startStep <= 4:
         printMsg("Completed Step 3")
         rcode=buildStep(4,jname,"")
    if rcode == 0: 
       if startStep <= 5:
         printMsg("Completed Step 4")
         rcode=buildStep(5,jname,resfname_prefix)
       if rcode == 0: printMsg("Completed Extracing Results")
    
    return rcode

def copyInputFile():
   printMsg("In copyInputFile")
   col1,col2, exp1 = readInp("|")
   retval=1
   fnamePrefix="Unknown"
   if int(exp1) == 3:
     retval,fnamePrefix = copyInputFileBySepar('|')
   else :
     col1,col2, exp1 = readInp("\t")
     if int(exp1) == 3:
       printMsg("copying Input File Tab Separated")
       retval, fnamePrefix = copyInputFileBySepar('\t')
     else:
       exp3 = readInp("\xb3")
       if int(exp3) == 3:
         print "Separator graphic | character"
         print "Either someone at Neustar OR Experian need to convert the data file"
         printMsg("Separator graphic | character")
         printMsg("Either someone at Neustar OR Experian need to convert the data file")
   return retval,fnamePrefix

def main():

 distdid=0
 distclid=0
 fnamePrefix="Unknown"
 txtfname = "/tmp/"+jname+".txt"
 print fname
 if os.access(fname,os.R_OK) == False:
   printMsg("File Permissions Are Not Set")
 else:
   retval = checkHeader("gzip -cd "+fname+" | grep \"MOBILE_AD_ID|\"")
   if retval != 0:
     retval = checkHeader("gzip -cd "+fname+" | grep \"DEVICE_ID|\"")
     if retval != 0:
       retval = checkHeader("gzip -cd "+fname+" | grep \"_maid\"")
     if retval != 0:
       retval = checkHeader("gzip -cd "+fname+" | grep \"_md5\"")
   if retval != 0:
     print("Data file header has to have MOBILE_AD_ID or DEVICE_ID as one of the columns")
     printMsg("Data file header has to have MOBILE_AD_ID or DEVICE_ID as one of the columns")
   else:
    if os.path.exists(txtfname) == False:
      getContent("gzip -cd "+fname+" | head  > /tmp/"+jname+".txt", "Cannot read "+fname)
    print os.path.isdir(experian_hdfs+jname) 
    print "Trying mkdir 1>>"
    jtdout, jstderr, returncode = runCommand("mkdir "+experian_hdfs+jname)
    if returncode == 0:
      print "Will launch data copy"
      returncode,fnamePrefix=copyInputFile()
    else:
      returncode=0
      pos = fname.rfind("/")
      resfname = fname[pos+1:]
      pos = resfname.find(".")
      fnamePrefix = resfname[0:pos]
      printMsg("Input Path in HDFS Already Exists")
    if returncode == 0:
      returncode = process_steps(fnamePrefix)
    if returncode == 0:
      distdid,distclid = showStats(jname)
    if distdid > 0 and distclid > 0 :
      runCommand("cp /tmp/"+jname+".txt "+workarea+"stats/"+jname+".txt")
      runCommand("grep -v WARN "+workarea+"runs/"+jname+"_step4.1 | grep -v \"_c0\" > "+workarea+"stats/"+jname+"_raw.txt")
      showFinalStat(jname, distdid, distclid)


if __name__ == '__main__':     # if the function is the main function ...
    start_time=time.time()

    main()

    end_time=time.time()
    hours, rem = divmod(end_time-start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    #print("Elapsed time")
    #print(str(int(hours)) +" hours "+ str(int(minutes))+" minutes "+str(seconds) + " seconds")
    printMsg("Elapsed time")
    printMsg(str(int(hours)) +" hours "+ str(int(minutes))+" minutes "+str(seconds) + " seconds")

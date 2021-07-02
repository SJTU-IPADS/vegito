#! /usr/bin/env python

## include packages
import commands
import subprocess # execute commands
import sys    # parse user input
import signal # bind interrupt handler

import time #sleep

import xml.etree.cElementTree as ET ## parsing the xml file

import threading #lock

import os  # change dir

#import zmq # network communications, if necessary

## user defined packages
from run_util import print_with_tag
from run_util import change_to_parent

#====================================#

ssh_port = "22"  # update it if use docker, as string

ignore = []

## config parameters and global parameters
mac_set = []

## benchmark constants
BASE_CMD = "./%s --bench %s --verbose --config %s"
#output_cmd = "1>/dev/null 2>&1 &" ## this will ignore all the output
OUTPUT_CMD_LOG = " 1>log 2>&1 &" ## this will flush the log to a file


## bench config parameters

base_cmd = ""
config_file = "config.xml"

## start parese input parameter"
program = "dbtest"

exe = ""
bench = "bank"
arg = ""

## lock
int_lock = threading.Lock()

# ending flag
script_end = False

#====================================#
## class definations
class GetOutOfLoop(Exception):
    ## a dummy class for exit outer loop
    pass

#====================================#
## helper functions
def copy_file(f):
    FNULL = open(os.devnull, 'w')
    for host in mac_set:
        subprocess.call(["scp", "-P", ssh_port, "%s" % f, "%s:%s" % (host,"~")], 
                        stdout=FNULL)
    FNULL.close()

def copy_files(flist):
    FNULL = open(os.devnull, 'w')
    cmd = ["scp", "-P", ssh_port] + flist
    for host in mac_set:
        # print_with_tag("SCP", "copy to %s" % host)
        subprocess.call(cmd + ["%s:%s" % (host, "~")], stdout=FNULL)
    FNULL.close()


def kill_servers():
    #  print "ending ... kill servers..."
    kill_cmd1 = "pkill %s --signal 2" % exe
    # real kill
    kill_cmd2 = "pkill %s" % exe
    for i in xrange(len(mac_set)):
        if (i in ignore): continue
        subprocess.call(["ssh", "-p", ssh_port, "-n","-f", mac_set[i], kill_cmd1])

    time.sleep(1)
    
    for i in xrange(len(mac_set)):
        if (i in ignore): continue
        subprocess.call(["ssh", "-p", ssh_port, "-n","-f", mac_set[i], kill_cmd2])
    
    time.sleep(1) ## ensure that all related processes are cleaned
    return

## singal handler
def signal_int_handler(signal, frame):

    global script_end
    int_lock.acquire()

    if (script_end):
        int_lock.release()
        return

    script_end = True

    print_with_tag("ENDING", "kill all processes in SIGINT handler")
    kill_servers()
    print_with_tag("ENDING", "kill processes done")
    int_lock.release()

    sys.exit(0)
    return

def parse_input():
    global config_file, exe,  base_cmd, arg, bench ## global declared parameters
    if (len(sys.argv)) > 1: ## config file name
        config_file = sys.argv[1]
    if (len(sys.argv)) > 2: ## exe file name
        exe = sys.argv[2]
    if (len(sys.argv)) > 3: ## program specified args
        args = sys.argv[3]
    if (len(sys.argv)) > 4:
        bench = sys.argv[4]
    base_cmd = (BASE_CMD % (exe, bench, config_file)) + " " + args + " --server-id %d "
    return


# print "starting with config file  %s" % config_file
def parse_bench_parameters(f):
    global mac_set, mac_num
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "bench"
    mac_num = int(root.find("servers").find("num").text)
    mac_set = []
    for e in root.find("servers").find("mapping").findall("a"):
        mac_set.append(e.text.strip())
    mac_set = mac_set[0 : mac_num]
    return

def start_servers(macset, config, bcmd):
    assert(len(macset) >= 1)
    FNULL = open(os.devnull, 'w')
    for i in xrange(1,len(macset)):
        if (i in ignore): continue
        cmd = (bcmd % (i)) + OUTPUT_CMD_LOG ## disable remote output
        subprocess.call(["ssh", "-p", ssh_port,"-n","-f",macset[i],"rm *.log"], 
                        stderr=FNULL) ## clean remaining log
        subprocess.call(["ssh", "-p", ssh_port, "-n","-f", macset[i], cmd])
    ## local process is executed right here
    ## cmd = "perf stat " + (bcmd % 0)
    FNULL.close()
    cmd = bcmd % 0
    print_with_tag("RUN", cmd)
    subprocess.call(cmd.split()) ## init local command for debug
    # print "!!!!!!!!!!!!!! Complete !!!!!!!!!!!!!!!!!!!!!!"
    #subprocess.call(["ssh", "-p", ssh_port,"-n","-f",macset[0],cmd])
    return

def prepare_files(files):
    copy_files(files)
    print_with_tag("COPY", "%s" % files)
    # for f in files:
    #     copy_file(f)
    #     print_with_tag("COPY", "%s" % f)
    return



#====================================#
## main function
def main():

    global base_cmd
    parse_input() ## parse input from command line
    parse_bench_parameters(config_file) ## parse bench parameter from config file
    print_with_tag("START", "Input parsing done.")
    print_with_tag("ARG", base_cmd % 0)
    print_with_tag("HOSTS", mac_set)

    kill_servers()
    print_with_tag("START", "Kill remaining processes done.")

    prepare_files([exe,config_file])    ## copy related files to remote

    signal.signal(signal.SIGINT, signal_int_handler) ## register signal interrupt handler
    start_servers(mac_set, config_file, base_cmd) ## start server processes
    # for i in xrange(10):
    #     ## forever loop
    #     time.sleep(10)
    signal_int_handler(0,1) ## 0,1 are dummy args
    return

#====================================#
## the code
if __name__ == "__main__":
    main()

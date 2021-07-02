#! /usr/bin/env python

## include packages
import commands
import subprocess # execute commands
import sys    # parse user input
import signal # bind interrupt handler

import time #sleep

import xml.etree.cElementTree as ET ## parsing the xml file

filename = "freshness.xml"

if len(sys.argv) < 3:
    print "Usage: %s <template-xml> <epoch-interval-ms>" % sys.argv[0]
    sys.exit(0)

filename = sys.argv[1]
sync_ms = int(sys.argv[2])
print "config: %s, epoch interval %d ms" % (filename, sync_ms)

raw_log_fn = "freshness-%02d-raw.log" % sync_ms
log_fn = "freshness-%02d.log" % sync_ms

tree = ET.parse(filename)
root = tree.getroot()
assert root.tag == "bench"
node = root.find("sync_ms")
node.text = " %d " % sync_ms
tree.write(filename,encoding='UTF-8',xml_declaration=True)

print "start to run VEGITO ..."
raw_log = open(raw_log_fn, 'w')
subprocess.call(["./run.sh", "8", filename], stdout=raw_log)
raw_log.close()

print "Data statistics ..."
log = open(log_fn, 'w')
subprocess.call(["grep", "freshness:", raw_log_fn], stdout=log)
log.close()

subprocess.call(["./fresh-eval.py", log_fn])

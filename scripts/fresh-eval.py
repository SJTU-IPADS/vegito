#!/usr/bin/env python

import sys

filename=sys.argv[1]
log_file = open(filename, 'r')
lines = log_file.readlines()
print len(lines)

line_num = 0
count = 0
lags = []

# line format:
# ['epoch:', '45', 'ms,', 'freshness:', '19.998000', 'ms']
epoch = 0
for line in lines:
    if line_num < len(lines) * 0.8 - 10000:
        line_num += 1
        continue

    if line_num > len(lines) * 0.8:
        break
    
    tokens = line.split()
    if epoch == 0:
        epoch = int(tokens[1])
        print "epoch interval: %d ms" % epoch
    # print tokens
    try:
        lags.append(float(tokens[4]))
    except ValueError:
        tmp_i = 0
        for token in tokens:
            tmp_i += 1
            if token == "freshness:":
                break
        lags.append(float(tokens[tmp_i]))
        
    line_num += 1

lags.sort()
max_lag = max(lags)
min_lag = min(lags)
avg_lag = sum(lags) / len(lags)
med_lag = lags[len(lags) / 2]
print "max: %f ms, max/epoch = %f" % (max_lag, max_lag / epoch)
print "min: %f ms, min/epoch = %f" % (min_lag, min_lag / epoch)
print "avg: %f ms, avg/epoch = %f" % (avg_lag, avg_lag / epoch)
print "med: %f ms, med/epoch = %f" % (med_lag, med_lag / epoch)

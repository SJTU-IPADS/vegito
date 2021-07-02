#!/usr/bin/env python3

import subprocess
import os
import sys
import signal
import time
import argparse

line_buf = []

# format line data and save it as a excel file
def sigint_handler(sig, frame):
    print('')
    for element in line_buf:
        line = element.split()
        print(line[1])

    sys.exit(0)

def eval(tree_type, num_writers, num_readers, balance_interval, read_type):
    process = subprocess.Popen('./index_test' + \
                                ' -t ' + tree_type + \
                               ' -w ' + str(num_writers) + \
                               ' -r ' + str(num_readers) + \
                               ' -m ' + 't' + \
                               ' -i ' + str(balance_interval) + \
                               ' -y ' + read_type,
                               shell = True,
                               stdout = subprocess.PIPE)

    count = 0
    for line in iter(process.stdout.readline, ''):
        str_line = line.decode('utf-8')
        print(str_line, end='')

        if count >= 12:
            line_buf.append(str_line)

        count += 1

def main():
    global wb_title

    parser = argparse.ArgumentParser(description="run_index_test.py")
    parser.add_argument('tree_type', type=str, choices=['b', 'p'], help='tree_type')
    parser.add_argument('num_writers', type=int)
    parser.add_argument('num_readers', type=int)
    parser.add_argument('balance_interval', type=float)
    parser.add_argument('read_type', type=str, choices=['r', 't'], help='read_type')

    args = parser.parse_args()

    signal.signal(signal.SIGINT, sigint_handler)

    eval(args.tree_type, args.num_writers, args.num_readers, args.balance_interval, args.read_type)

if __name__ == '__main__':
    main()

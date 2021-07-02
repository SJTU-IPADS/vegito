#!/usr/bin/env bash

target="$1"
## this script will sync the project to the remote server
rsync -rtuv $PWD $target:~ --exclude .git/ --exclude ./pre-data/ --exclude '.*.swp'


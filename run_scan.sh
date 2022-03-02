#!/bin/bash
#PBS -q normal
#PBS -l ncpus=2
#PBS -l walltime=3:00:00
#PBS -l mem=8gb
#PBS -l jobfs=20gb
#PBS -l wd
#PBS -j oe
# - PBS -m e


set -euo pipefail

module use /g/data/hh5/public/modules

module load parallel
module load conda

#set -x

PROJECTS="p66 hh5/tmp v45 w40 w42 w97 ly62"

sed -e 's:\<\(\S\+\):/g/data/\1 /scratch/\1:g' -e 's:\s\+:\n:g' <<< $PROJECTS | parallel -v --jobs 4 python src/grafanadb/dusql_scan.py {} --output $TMPDIR/'dusql.{= $_=~ s:/:_:g =}.csv'

./run_update.sh

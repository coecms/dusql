#!/bin/bash
#PBS -q normal
#PBS -l ncpus=2
#PBS -l walltime=1:00:00
#PBS -l mem=2gb
#PBS -l jobfs=20gb
#PBS -l wd


set -euo pipefail

module load parallel
module load conda

#set -x

PROJECTS="w35 w40 v45 w42 w48 w97 hh5"
PROJECTS="hh5/tmp"

sed -e 's:\<\(\S\+\):/g/data/\1 /short/\1:g' -e 's:\s\+:\n:g' <<< $PROJECTS | parallel -v --jobs 4 python src/grafanadb/dusql_scan.py {} --output $TMPDIR/'dusql.{= $_=~ s:/:_:g =}.csv'

cp $TMPDIR/dusql.*.csv /g/data/w35/saw562/dusql

cat > $TMPDIR/dusql_update_head <<EOF
BEGIN;

DELETE FROM dusql_inode;

DROP INDEX IF EXISTS dusql_inode_parent;
DROP INDEX IF EXISTS dusql_inode_inode;

COPY dusql_inode(
    parent_inode,
    parent_device,
    inode,
    device,
    mode,
    uid,
    gid,
    size,
    mtime,
    scan_time,
    basename
) FROM STDIN WITH (FORMAT CSV);
EOF

cat > $TMPDIR/dusql_update_tail <<EOF
\.

CREATE INDEX dusql_inode_parent ON dusql_inode(parent_device, parent_inode, basename);
CREATE INDEX dusql_inode_inode ON dusql_inode(device, inode);
COMMIT;
EOF

ssh -NL 9876:localhost:5432 jenkins &
tunnelid=$!

trap "{ kill $tunnelid; }" EXIT

sleep 2

time psql -h localhost -p 9876  -d grafana -f <(cat $TMPDIR/dusql_update_head $TMPDIR/dusql.*.csv $TMPDIR/dusql_update_tail)


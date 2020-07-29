#!/bin/bash
#PBS -q normal
#PBS -l ncpus=2
#PBS -l walltime=3:00:00
#PBS -l mem=8gb
#PBS -l jobfs=20gb
#PBS -l wd
#PBS -j oe
#PBS -m e


set -euo pipefail

module use /g/data/hh5/public/modules

module load parallel
module load conda

#set -x

PROJECTS="hh5/tmp v45 w35 w40 w42 w48 w97 ly62"
#PROJECTS="w35"

sed -e 's:\<\(\S\+\):/g/data/\1 /scratch/\1:g' -e 's:\s\+:\n:g' <<< $PROJECTS | parallel -v --jobs 4 python src/grafanadb/dusql_scan.py {} --output $TMPDIR/'dusql.{= $_=~ s:/:_:g =}.csv'

cp $TMPDIR/dusql.*.csv /g/data/w35/saw562/dusql


cat > $TMPDIR/dusql_update_head <<EOF
BEGIN;

DROP TABLE IF EXISTS dusql_inode CASCADE;

CREATE UNLOGGED TABLE dusql_inode (
        basename TEXT NOT NULL,
        inode BIGINT,
        device BIGINT NOT NULL,
        mode INTEGER,
        uid INTEGER,
        gid INTEGER,
        size BIGINT,
        mtime FLOAT,
        atime FLOAT,
        scan_time FLOAT,
        root_inode BIGINT,
        parent_inode BIGINT
);

COPY dusql_inode(
    inode,
    device,
    mode,
    uid,
    gid,
    size,
    mtime,
    atime,
    scan_time,
    basename,
    root_inode,
    parent_inode
) FROM STDIN WITH (FORMAT CSV);
EOF

cat > $TMPDIR/dusql_update_tail <<EOF
\.

COMMIT;
EOF

ssh -NL 9876:localhost:5432 jenkins &
tunnelid=$!

trap "{ kill $tunnelid; }" EXIT

sleep 2

time psql -h localhost -p 9876  -d grafana -f <(cat $TMPDIR/dusql_update_head /g/data/w35/saw562/dusql/dusql.*.csv $TMPDIR/dusql_update_tail sql/dusql_schema.sql)


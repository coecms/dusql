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

module load conda


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

# Tunnel to the jenkins server
ssh -W 10.0.3.190 -NL 9876:localhost:5432 accessdev.nci.org.au &
tunnelid=$!

trap "{ kill $tunnelid; }" EXIT

sleep 2

time psql -h localhost -p 9876  -d grafana -f <(cat $TMPDIR/dusql_update_head $TMPDIR/dusql.*.csv $TMPDIR/dusql_update_tail sql/dusql_schema.sql)


/*
 * Stores information about the current state of the filesystem
 * Information comes from running stat() on each file
 */
CREATE UNLOGGED TABLE IF NOT EXISTS dusql_inode (
        basename TEXT NOT NULL,
        inode BIGINT,
        device BIGINT,
        mode INTEGER,
        uid INTEGER,
        gid INTEGER,
        size BIGINT,
        mtime FLOAT,
        scan_time FLOAT,
        root_inode BIGINT, -- inode of directory that was scanned to find this file
        parent_inode BIGINT -- inode of this file's parent directory
);

/*
 * Stores a summary of historical filesystem state
 */
CREATE TABLE IF NOT EXISTS dusql_history (
    id SERIAL PRIMARY KEY,
    root_inode BIGINT,
    uid INTEGER,
    gid INTEGER,
    inodes INTEGER,
    size BIGINT,
    min_age INTERVAL,
    time TIMESTAMP WITH TIME ZONE);


CREATE OR REPLACE FUNCTION dusql_path_func(search_parent_inode BIGINT, search_device BIGINT, search_basename TEXT) RETURNS TEXT AS $$
        WITH RECURSIVE x AS (
                SELECT
                        search_parent_inode AS inode,
                        0 AS depth
                UNION ALL
                SELECT
                        dusql_parent.parent_inode AS inode,
                        depth + 1 AS depth
                FROM
                        x
                JOIN    dusql_parent
                ON      x.inode = dusql_parent.inode
                AND     search_device = dusql_parent.device
        )
        SELECT
                string_agg(basename, '/') || '/' || search_basename AS path
        FROM (
                SELECT
                        dusql_inode.basename AS basename
                FROM
                        dusql_inode
                JOIN    x
                ON      x.inode = dusql_inode.inode
                AND     search_device = dusql_inode.device
                ORDER BY depth DESC
        ) AS y
$$ LANGUAGE SQL;


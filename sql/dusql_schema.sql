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
CREATE INDEX IF NOT EXISTS dusql_inode_id ON dusql_inode(device, inode);

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

/*
 * Create a path given (parent_inode, device, basename) of an inode
*/
CREATE OR REPLACE FUNCTION dusql_path_func(search_parent_inode BIGINT, search_device BIGINT, search_basename TEXT) RETURNS TEXT AS $$
        WITH RECURSIVE x AS (
                SELECT
                        search_parent_inode AS parent_inode,
                        search_basename AS basename,
                        0 AS depth
                UNION ALL
                SELECT
                        dusql_inode.parent_inode AS parent_inode,
                        dusql_inode.basename AS basename,
                        depth + 1 AS depth
                FROM
                        x
                JOIN    dusql_inode
                ON      x.parent_inode = dusql_inode.inode
                AND     search_device = dusql_inode.device
        )
        SELECT string_agg(basename, '/') AS path
        FROM (
                SELECT basename FROM x
                ORDER BY depth DESC
        ) AS y
$$ LANGUAGE SQL;


/*
 * Bin the dusql database by root id, uid, gid and age to store historical information
 */
WITH 
    x AS (
        SELECT p FROM (
            VALUES
            (INTERVAL 'P0D'),
            (INTERVAL 'P1M'),
            (INTERVAL 'P6M'),
            (INTERVAL 'P1Y'),
            (INTERVAL 'P3Y')
        ) AS v(p)
    ),
    y AS (
        SELECT TSTZRANGE(CURRENT_TIMESTAMP - LEAD(p,1) OVER(ORDER BY p), CURRENT_TIMESTAMP - p), p 
        FROM x
    )
    INSERT INTO dusql_history(root_inode, uid, gid, inodes, size, min_age, time)
        SELECT root_inode, uid, gid, COUNT(*) AS inodes, SUM(size) AS size, p AS min_age, CURRENT_TIMESTAMP
        FROM dusql_inode
        JOIN y ON tstzrange @> TO_TIMESTAMP(mtime)
        GROUP BY root_inode, uid, gid, p;

WITH 
    x AS (
        SELECT p FROM (
            VALUES
            (INTERVAL 'P0D'),
            (INTERVAL 'P60D'),
            (INTERVAL 'P90D'),
        ) AS v(p)
    ),
    y AS (
        SELECT TSTZRANGE(CURRENT_TIMESTAMP - LEAD(p,1) OVER(ORDER BY p), CURRENT_TIMESTAMP - p), p 
        FROM x
    )
    INSERT INTO dusql_access(root_inode, uid, gid, inodes, size, last_access, time)
        SELECT root_inode, uid, gid, COUNT(*) AS inodes, SUM(size) AS size, p AS last_access, CURRENT_TIMESTAMP
        FROM dusql_inode
        JOIN y ON tstzrange @> TO_TIMESTAMP(atime)
        GROUP BY root_inode, uid, gid, p;

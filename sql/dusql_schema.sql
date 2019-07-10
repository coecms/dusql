CREATE TABLE IF NOT EXISTS dusql_inode (
	id SERIAL PRIMARY KEY,
	basename TEXT NOT NULL,
	parent_inode BIGINT,
	parent_device BIGINT,
	inode BIGINT,
	device BIGINT,
	mode INTEGER,
	uid INTEGER,
	gid INTEGER,
	size BIGINT,
	mtime FLOAT,
	scan_time FLOAT
);

CREATE INDEX IF NOT EXISTS dusql_inode_mode ON dusql_inode (mode);
CREATE INDEX IF NOT EXISTS dusql_inode_inode ON dusql_inode (device, inode);
CREATE INDEX IF NOT EXISTS dusql_inode_parent ON dusql_inode (parent_device, parent_inode);

CREATE OR REPLACE VIEW dusql_parent AS
	SELECT
		dusql_inode.id AS id,
		parent.id AS parent_id
	FROM
		dusql_inode
	JOIN	dusql_inode AS parent
	ON	dusql_inode.parent_device = parent.device
	AND	dusql_inode.parent_inode = parent.inode
;

CREATE OR REPLACE FUNCTION dusql_path_func(search_id INTEGER) RETURNS TEXT AS $$
	WITH RECURSIVE x AS (
		SELECT
			search_id AS id,
			0 AS depth
		UNION ALL
		SELECT
			dusql_parent.parent_id AS id,
			depth + 1 AS depth
		FROM
			x
		JOIN	dusql_parent ON x.id = dusql_parent.id
	)
	SELECT
		string_agg(bASename, '/') AS path
	FROM (
		SELECT
			dusql_inode.bASename AS bASename
		FROM
			dusql_inode
		JOIN	x
		ON	x.id = dusql_inode.id
		ORDER BY depth DESC
	) AS y
$$ LANGUAGE SQL;


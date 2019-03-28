"""splitpaths

Revision ID: 99a0585c3dfa
Revises: 203b23add962
Create Date: 2019-03-28 10:51:56.938690

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '99a0585c3dfa'
down_revision = '203b23add962'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('DROP VIEW paths_descendants')
    op.execute('DROP VIEW paths_parents')
    op.execute('DROP VIEW paths_fullpath')
    op.execute('DROP TABLE paths_closure')
    op.execute('DROP VIEW paths_parent_id')

    with op.batch_alter_table('paths') as bop:
        bop.add_column(sa.Column('parent_device', sa.Integer))
        bop.alter_column('links', type_=sa.Integer)
        bop.drop_constraint('uniq_inode_edge')

    op.rename_table('paths', 'paths_ingest')

    op.create_table('basenames',
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text, unique=True, index=True),
            )

    op.create_table('paths',
            sa.Column('id',sa.Integer, primary_key=True, index=True),
            sa.Column('parent_id',sa.Integer,sa.ForeignKey('paths.id'), index=True),
            sa.Column('basename_id',sa.Integer,sa.ForeignKey('basenames.id')),

            sa.Column('inode',sa.Integer),
            sa.Column('device', sa.Integer),
            sa.Column('parent_inode',sa.Integer),
            sa.Column('parent_device',sa.Integer),

            sa.Column('size',sa.Integer),
            sa.Column('mtime',sa.Float),
            sa.Column('ctime',sa.Float),
            sa.Column('uid', sa.Integer),
            sa.Column('gid', sa.Integer),
            sa.Column('mode', sa.Integer),
            sa.Column('links', sa.Integer),
            sa.Column('last_seen',sa.Float),

            sa.Index('idx_paths_inode', 'inode', 'device', 'parent_inode', 'parent_device', 'basename_id', unique=True),
            )

    op.execute("""
        CREATE VIRTUAL TABLE paths_closure
        USING transitive_closure(
            tablename=paths,
            idcolumn=id,
            parentcolumn=parent_id)
        """)

    op.execute("""
        CREATE VIEW paths_descendants AS
        SELECT
           paths.id AS path_id,
           paths_closure.id AS desc_id,
           paths_closure.depth AS depth
        FROM paths_closure
        JOIN paths ON root = paths.id
        """)

    op.execute("""
        CREATE VIEW paths_parents AS
        SELECT
           paths.id AS path_id,
           paths_closure.id AS parent_id,
           paths_closure.depth AS depth
        FROM paths_closure
        JOIN paths ON root = paths.id
        WHERE idcolumn = 'parent_id'
          AND parentcolumn = 'id'
        """)

    op.execute("""
        CREATE VIEW paths_fullpath AS
        WITH RECURSIVE s(path_id, path) AS (
            SELECT
                root_paths.path_id AS path_id,
                root_paths.path AS path
            FROM root_paths
            LEFT JOIN paths ON root_paths.path_id = paths.id
            WHERE paths.parent_id IS NULL
        UNION ALL
            SELECT
                paths.id as path_id,
                parent.path || '/' || basenames.name AS path
            FROM paths
            JOIN basenames ON paths.basename_id = basenames.id
            JOIN s AS parent on parent.path_id = paths.parent_id
        )
        SELECT path_id, path FROM s
        """)

    op.execute("""
        INSERT OR IGNORE INTO basenames(name)
        SELECT name FROM paths_ingest
    """)

    op.execute("""
        INSERT INTO paths (
            id,
            basename_id,
            inode,
            device,
            parent_inode,
            parent_device,
            size,
            mtime,
            ctime,
            uid,
            gid,
            mode,
            links,
            last_seen
        ) 
        SELECT 
            paths_ingest.id,
            basenames.id,
            paths_ingest.inode,
            paths_ingest.device,
            paths_ingest.parent_inode,
            COALESCE(
                paths_ingest.parent_device,
                paths_ingest.device
                ),
            paths_ingest.size,
            paths_ingest.mtime,
            paths_ingest.ctime,
            paths_ingest.uid,
            paths_ingest.gid,
            paths_ingest.mode,
            paths_ingest.links,
            paths_ingest.last_seen
        FROM paths_ingest
        JOIN basenames ON paths_ingest.name == basenames.name
        WHERE true
        ON CONFLICT (
            basename_id,
            inode,
            device,
            parent_inode,
            parent_device
        )
        DO UPDATE SET
            size = excluded.size,
            mtime = excluded.mtime,
            ctime = excluded.ctime,
            uid = excluded.uid,
            gid = excluded.gid,
            mode = excluded.mode,
            links = excluded.links,
            last_seen = excluded.last_seen
    """)

    op.execute("""
        UPDATE paths
        SET parent_id = (
            SELECT id
            FROM paths as parent
            WHERE
                parent.inode == paths.parent_inode AND
                parent.device == paths.parent_device
        ) WHERE parent_id IS NULL
    """)

    op.execute("DELETE FROM paths_ingest")


def downgrade():
    pass

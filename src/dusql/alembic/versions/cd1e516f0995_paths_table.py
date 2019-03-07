"""paths table

Revision ID: cd1e516f0995
Revises:
Create Date: 2019-03-07 12:53:28.773122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cd1e516f0995'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('paths',
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('name',sa.String),
        sa.Column('inode',sa.Integer,index=True),
        sa.Column('size',sa.Integer),
        sa.Column('mtime',sa.Float),
        sa.Column('ctime',sa.Float),
        sa.Column('parent_inode',sa.Integer,index=True),
        sa.Column('uid', sa.Integer),
        sa.Column('gid', sa.Integer),
        sa.Column('mode', sa.Integer),
        sa.Column('device', sa.Integer),
        sa.Column('last_seen',sa.Float),
        sa.Column('mdss_state',sa.String),
        sa.Column('links',sa.String),
        sa.UniqueConstraint('inode','parent_inode','name',name='uniq_inode_edge'),
        )

    op.execute("""
        CREATE VIEW paths_parent_id AS
        SELECT
            paths.id as id,
            parent.id as parent_id
        FROM paths
        JOIN paths AS parent
            ON paths.parent_inode = parent.inode
        """)

    op.execute("""
        CREATE VIRTUAL TABLE paths_closure
        USING transitive_closure(
            tablename=paths_parent_id,
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
        WITH s AS (
            SELECT
                paths_parents.path_id,
                paths.name
            FROM paths
            JOIN paths_parents
                ON paths.id = paths_parents.parent_id
            ORDER BY
                paths_parents.path_id,
                paths_parents.depth DESC
        )
        SELECT
            s.path_id AS path_id,
            group_concat(s.name, '/') AS path
        FROM s
        GROUP BY s.path_id
        """)


def downgrade():
    pass

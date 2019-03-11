"""Full paths

Revision ID: 203b23add962
Revises: cd1e516f0995
Create Date: 2019-03-11 09:38:21.606132

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '203b23add962'
down_revision = 'cd1e516f0995'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('root_paths',
        sa.Column('path_id', sa.Integer, sa.ForeignKey('paths.id'), primary_key=True),
        sa.Column('path', sa.Text),
        )

    op.execute("""
        DROP VIEW paths_fullpath
        """)

    op.execute("""
        CREATE VIEW paths_fullpath AS
        WITH RECURSIVE s(path_id, path) AS (
            SELECT
                root_paths.path_id AS path_id,
                root_paths.path AS path
            FROM root_paths
            LEFT JOIN paths_parent_id ON root_paths.path_id = paths_parent_id.id
            WHERE paths_parent_id.parent_id IS NULL
        UNION ALL
            SELECT
                paths.id as path_id,
                parent.path || '/' || paths.name AS path
            FROM paths
            JOIN paths_parent_id ON paths.id = paths_parent_id.id
            JOIN s AS parent on parent.path_id = paths_parent_id.parent_id
        )
        SELECT path_id, path FROM s
        """)


def downgrade():
    pass

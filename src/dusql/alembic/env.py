from __future__ import with_statement

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

context.configure(
    connection=config.attributes['connection'],
)

with context.begin_transaction():
    context.run_migrations()

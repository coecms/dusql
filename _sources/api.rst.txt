Python Functions
================

Connecting to the database
--------------------------

The Dusql python interface is build around a SQLAlchemy database connection.
Call :func:`dusql.db.connect` before anything else, and pass the connection
object it returns to the other functions.

.. autofunction:: dusql.db.connect

Configuration
-------------

.. autodata:: dusql.config.schema
    :annotation:
.. autodata:: dusql.config.defaults
    :annotation:
.. autofunction:: dusql.config.get_config

Importing paths
---------------

.. autofunction:: dusql.scan.scan

.. autofunction:: dusql.scan.autoscan

Finding files
-------------

.. autofunction:: dusql.find.find

Reporting Usage
---------------

.. autofunction:: dusql.report.report

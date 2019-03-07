dusql - SQL Based Disk Usage Analyser
================================================================================

.. image:: https://img.shields.io/travis/com/coecms/dusql/master.svg
    :target: https://travis-ci.com/coecms/dusql
    :alt: Build Status
.. image:: https://img.shields.io/codacy/grade/427f425167b34f1a88c0d352e2709e52.svg
    :target: https://www.codacy.com/app/ScottWales/dusql
    :alt: Code Style
.. image:: https://img.shields.io/codacy/coverage/427f425167b34f1a88c0d352e2709e52/master.svg
    :target: https://www.codacy.com/app/ScottWales/dusql
    :alt: Code Coverage
.. image:: https://img.shields.io/conda/v/coecms/dusql.svg
    :target: https://anaconda.org/coecms/dusql
    :alt: Conda

Scan all files under ``$DIR`` into the database::

    dusql scan $DIR

Print a summary of disk usage under ``$DIR``::

    dusql report $DIR

Check usage under ``$DIR`` for potential issues (TODO)::

    dusql report --check=all $DIR

Find files under ``$DIR``::

    dusql find --older_than 1y --group w35 $DIR

Configuration
-------------

``dusql`` reads a yaml config file from ``$HOME/.config/dusql.yaml`` to set the
database path etc.

To see the curret configuration run::

    dusql print-config

TODO:
-----

* Add check reports
* Handle multiple paths in cli arguments
* Update scans, handle deleted files and changed sizes
* Checks idea: temporary files of failed NCO/CDO commands
* Add more find arguments, e.g. size, mode

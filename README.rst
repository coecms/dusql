dusql - SQL Based Disk Usage Analyser
================================================================================

.. image:: https://travis-ci.com/coecms/dusql.svg?branch=master
    :target: https://travis-ci.com/coecms/dusql
.. image:: https://api.codacy.com/project/badge/Grade/427f425167b34f1a88c0d352e2709e52
    :target: https://www.codacy.com/app/ScottWales/dusql

Scan all files under ``$DIR`` into the database::

    dusql scan $DIR

Print a summary of disk usage under ``$DIR``::

    dusql report $DIR

Check usage under ``$DIR`` for potential issues (TODO)::

    dusql report --check=all $DIR

Find files under ``$DIR``::

    dusql find --older-than P1Y --group w35 $DIR


TODO:

 * Add tests
 * Add check reports
 * Handle multiple paths
 * Automatically scan path if not in directory
 * Update scans, handle deleted files
 * Change database path

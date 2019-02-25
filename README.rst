dusql - SQL Based Disk Usage Analyser
================================================================================

.. image:: https://travis-ci.com/coecms/dusql.svg?branch=master
    :target: https://travis-ci.com/coecms/dusql

Scan all files under ``$DIR`` into the database::

    dusql scan $DIR

Print a summary of disk usage under ``$DIR``::

    dusql report $DIR

Check usage under ``$DIR``::

    dusql report --check=all $DIR

Find files under ``$DIR``::

    dusql find --older-than P1Y --group w35 $DIR

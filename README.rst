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

Scan all files under ``$DIR`` into the database (or update existing records
under ``$DIR``)::

    $ dusql scan $DIR

Print a summary of disk usage scanned into the database::

    $ dusql report
    Tags:
        umdata                    11.7 gb    65266
        conda                      8.8 gb   263338
    Scanned Paths:
        /home/562/saw562
            saw562   w35           1.4 gb    18179
            saw562   w48           0.0 gb        4
            saw562   S.U           0.0 gb       29
        /short/w35/saw562
            saw562   w35        1278.4 gb   659444
            saw562   w48         158.2 gb     2737
            hxw599   w48           6.4 gb       16
        mdss://w35/saw562
            saw562   w35         305.3 gb        6

Find files under ``$DIR``::

    $ dusql find --older_than 1y --group w35 $DIR

Configuration
-------------

``dusql`` reads a yaml config file from ``$HOME/.config/dusql.yaml`` to set the
database path etc.

To see the curret configuration run::

    dusql print-config

Tagging Directories
-------------------

You can tag directories to add summaries to ``dusql report``. This is done
through the config file, a sample configuration is::

    tags:
        umdata:
            paths:
              - /short/w35/saw562/UM_ROUTDIR
              - /short/w35/saw562/cylc-run
        conda:
            paths:
              - /short/w35/saw562/conda

TODO:
-----

* Add check reports
* Handle multiple paths in cli arguments
* Checks idea: temporary files of failed NCO/CDO commands
* Add more find arguments, e.g. size, mode

Postgres-based disk usage analyser
==================================

Commands
--------

* `dusql find <PATH>`: Lists files matching some constraints
* `dusql du <PATH>`: Show total size and number of files matching some constraints

Only files using CLEX storage on /short and /g/data are monitored

Constraints may be:
* `--size`: Files bigger than this value (use negative values for smaller than)
* `--mtime`: Files created after this date (use negative values for before this
  date). The date can be a year `2018`, date `20180623`, or a time delta
  readable by Pandas `1y6m` in which case the delta is subtracted from today.
* `--user`: Files owned by this user (use negative for not owned by this user)
* `--group`: Files owned by this group (use negative for not owned by this group)

`dusql du` can be used to list multiple directories, e.g. find where files smaller than 10 mb are:

```
$ dusql du /short/w35/saw562/*/ --size=-10mb | sort -hr
 10.22GB,   123475 files, /short/w35/saw562/scratch/
  7.85GB,    68668 files, /short/w35/saw562/cylc-run/
  5.04GB,   218170 files, /short/w35/saw562/conda/
```


Structure
---------

Server `grafanadb.server:app` on CMS Jenkins server:
* Runs flask webapp that talks to the database
* Configured using Accessdev puppet infrastructure

The server is installed in a conda environment in Scott's home directory on the Jenkins server, update with `conda update -c coecms dusqlpg`

Client program `dusql` on raijin/vdi:
* Talks to the webapp to get information from the database

Authentication
--------------

The webapp uses a key that's only readable by CLEX groups on Raijin/VDI, so only group members can see the disk information

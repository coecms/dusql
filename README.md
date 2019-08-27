Postgres-based disk usage analyser
==================================

Commands
--------

`dusql find <PATH>`: Lists files matching some constraints
`dusql du <PATH>`: Show total size and number of files matching some constraints

Only files using CLEX storage on /short and /g/data are monitored

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

Client program `dusql` on raijin/vdi:
* Talks to the webapp to get information from the database

Authentication
--------------

The webapp uses a key that's only readable by CLEX groups on Raijin/VDI, so only group members can see the disk information

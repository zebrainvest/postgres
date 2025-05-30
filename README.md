# Zebrainvest PostgreSQL database (propertytoolkit)

## Servers

* Database Server running the propertytoolkit database (PostgreSQL v.15.12 on Ubuntu 22.04).
* Archival Server keeping backups and arhival files, running PHP applications.

See [Servers](https://github.com/zebrainvest/postgres/wiki/Servers)

## Production Database Schema

* Database name is propertytoolkit.
* At the time of this writing, the database was 125GB large (about half of the size of the disk)
* It has the pg_cron and pg_postgis extensions installed

See [propertytoolkit database](https://github.com/zebrainvest/postgres/wiki/DB-propertytoolkit)

Find a schema dump in schema_dump.sql file.

## Cron jobs

There are two types of cron jobs:
* system cron jobs (crontab, /etc/cron*/* files), and
* database pg_cron jobs, handled via the pg_cron extension in the database

See [Cron Jobs](https://github.com/zebrainvest/postgres/wiki/Cron-Jobs)

## Stored procedures

* [update_listing_scrapper_data](https://github.com/zebrainvest/postgres/wiki/update_listing_scrapper_data)

# Zebrainvest PostgreSQL database (crawler_db)

## Server

* public IP address is 209.209.11.211
* 8 x CPUs, Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz
* 32GB RAM
* 260GB disk (252GB used, 8GB free)
* PostgreSQL

## Archival server

* public IP address is 188.40.58.201 (user property)
* 12 x CPUs, Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
* 256GB RAM
* 118TB disk (80TB used, 32TB free)
* PHP application/website

## Production Database Schema

* Database name is propertytoolkit.
* At the time of this writing, the database was 125GB large (about half of the size of the disk)

Find a schema dump in schema_dump.sql file.

## Cron jobs

* `sh ~/backup_and_rsync.sh`
* * runs at midnight every 7 days based on the day of the month (e.g., 1st, 8th, 15th, 22nd, 29th), not strictly once a week.
* * logs its output to the console (output is received by root user via email)
* * empties the `/root/property/backup_data` folder, and then dumps the propertytoolkit database in compressed custom format (pg_dump -F c) to it, with the name `propertytoolkit_backup_$(date +%Y%m%d).dump`
* * copies the dump to the Archival server, to the `/home/property/crawler_db` folder; it does not delete any files from it.
* * at the time of this writing, each dump is 17GB
* `/root/bin/update_listing_scrapper_data &`
* * runs every two hours
* * executes the stored procedure `update_listing_scrapper_data(1000)`
* * logs its output to `/var/log/cron/update_listing_scrapper_data.log`


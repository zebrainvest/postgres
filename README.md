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

## Tables

### Schema public

* air_availability (133,636,483)
* air_listing_map (10,458,426)
* air_listings (340,161)
* air_price (8,921,842)
* air_status (446,156)
* brand_aliases (38,991)
* brand_aliases_backup (2,920,003)
* certificates_depc (27,369,071)
* certificates_non_depc (1,351,384)
* cities (85)
* hmo_index (2,439,275)
* hmo_listings_afs (22,294)
* hmo_listings_spr (386,637)
* hmo_listings_uni (39,271)
* listing_scrapper_data (2,920,651)
* ni_listings (0)
* ni_results (0)
* ni_updates (0)
* old_price_paid (29,930,500)
* osopenuprn (40,825,714)
* outcodes (2,951)
* price_paid (30,155,887)
* price_paid_uprn_matches (29,538,226)
* propertytool (8,115,555)
* proxies (43)
* rightmove_region (475)
* target_proxies (123)
* uk_locations (7,834)
* updated_property_status (14,124,474)
* uprn_addresses (18,686,104)
* uprn_records (40,744,085)

### Schema eth

* dup_ppid (29,999)
* pp_addresses (70,445)
* pp_counties (132)
* pp_districts (467)
* pp_localities (23,800)
* pp_postcodes (1,312,548)
* pp_streets (324,423)
* pp_towns (1,156)
* price_paid_data (104,693)

## Cron jobs

### via pg_cron extension

* `CALL update_lease_years();`  -- jobid 3 (listing_update_lease_years)
* * runs at 02:00
* `CALL update_brand_aliases();`  -- jobid 14 (daily_update_brand_aliases)
* * runs at 02:00
* `CALL clean_listing_brandname();`  -- jobid 6 (clean_listing_brandname_daily)
* * runs at 03:00
* `CALL update_brand_names()`  -- jobid 15
* * runs at 03:00
* `CALL update_listing_property_types_daily();`  -- jobid 1 (flats and studios property types)
* * runs at 04:00
* `CALL update_leasehold_tenure()`  -- jobid 5
* * runs at 04:00
* `CALL update_semi_detached_to_house()`  -- jobid 7
* * runs at 04:00
* `CALL update_mobile_property_types()`  -- jobid 8
* * runs at 05:00
* `CALL update_hmo_property_types()`  -- jobid 9
* * runs at 06:00
* `CALL update_house_property_types()`  -- jobid 10
* * runs at 07:00
* `CALL update_flat_property_types()`  -- jobid 11
* * runs at 08:00
* `CALL update_lease_years_remaining()`  -- jobid 12
* * runs at 09:00
* `CALL update_shared_ownership_flag()`  -- jobid 13
* * runs at 10:00
* `CALL update_minsizeft_from_text();`  -- jobid 4 (update_minsizeft_job)
* * runs at 23:00

To regenerate the list above:
```
SELECT string_agg(
    format(E'* `%s`  -- jobid %s%s\n* * runs %s', command, jobid, ' ('||jobname||')',
        CASE WHEN (s[3], s[4], s[5]) = ('*', '*', '*') AND s[1] ~ '^[0-9]+$' AND s[2] ~ '^[0-9]+$'
            THEN format('at %s:%s', trim(to_char(s[2]::int, '00')), trim(to_char(s[1]::int, '00')))
        ELSE 'on '||schedule END)
    , E'\n' ORDER BY
        CASE WHEN s[2] ~ '^[0-9]+$' THEN s[2]::int END,
        CASE WHEN s[1] ~ '^[0-9]+$' THEN s[1]::int END,
        s[2], s[1], jobid)
FROM cron.job
CROSS JOIN LATERAL string_to_array(schedule, ' ') s
```

### via root's cron

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


-- Dumped from database version 15.12 (Ubuntu 15.12-1.pgdg22.04+1)
-- Dumped by pg_dump version 15.12 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA pg_catalog;


CREATE SCHEMA eth;


CREATE COLLATION public.ignore_punct_case (provider = icu, deterministic = false, locale = 'und-u-ka-shifted-ks-level1');


CREATE COLLATION public.num_ignore_case (provider = icu, deterministic = false, locale = 'und-u-kn-ks-level1');


CREATE COLLATION public.num_ignore_punct_case (provider = icu, deterministic = false, locale = 'und-u-ka-shifted-kn-ks-level1');


CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA public;


CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


CREATE PROCEDURE eth.create_price_paid()
    LANGUAGE plpgsql
    AS $$
DECLARE
	_r record;
	_cnt int;
	_total int;
	_i int = 0;
	_address_id int;
BEGIN
	-- CALL eth.initialise_lookups();

	--TRUNCATE TABLE eth.pp_addresses, eth.price_paid_data;
	
	RAISE NOTICE 'Determining address groups (based on start of postcode)...';
	CREATE TEMP TABLE split_parts AS
		SELECT split_part(upper(postcode), ' '::text, 1)
		FROM (SELECT * FROM public.price_paid LIMIT 10) x
		GROUP BY 1;
	CREATE INDEX ON pg_temp.split_parts (split_part);

	_total := count(*)::int FROM pg_temp.split_parts;
	RAISE NOTICE 'Inserting % groups of address records', _total;
	
	CREATE TEMP TABLE addresses (
		ppid uuid NOT NULL, sold_date date NOT NULL, price_paid int NOT NULL,
		postcode_id int, street_id int,
		county_id smallint, district_id smallint,
		town_id smallint, locality_id smallint,
		poan text, soan text, uprn bigint, latitude float, longitude float
	);

	FOR _r IN SELECT * FROM pg_temp.split_parts ORDER BY 1
	LOOP
		_address_id := COALESCE((SELECT max(address_id)::int FROM eth.pp_addresses), 0);
		_i := _i + 1;
		_cnt := COUNT(*) FROM public.price_paid WHERE split_part(upper(postcode), ' '::text, 1) = _r.split_part;
		
		RAISE NOTICE 'Updating lookup tables for "%" (batch % of % - % rows)', _r.split_part, _i, _total, _cnt;

		CREATE TEMP TABLE lookups ON COMMIT DROP AS
		SELECT
			array_agg(DISTINCT trim(postcode COLLATE ignore_punct_case))
				FILTER (WHERE
					postcode > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_postcodes WHERE postcode = pp.postcode)) AS postcodes,
			array_agg(DISTINCT trim(street COLLATE ignore_punct_case))
				FILTER (WHERE
					street > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_streets WHERE street = pp.street)) AS streets,
			array_agg(DISTINCT trim(locality COLLATE ignore_punct_case))
				FILTER (WHERE
					locality > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_localities WHERE locality = pp.locality)) AS localities,
			array_agg(DISTINCT trim(town COLLATE ignore_punct_case))
				FILTER (WHERE
					town > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_towns WHERE town = pp.town)) AS towns,
			array_agg(DISTINCT trim(district COLLATE ignore_punct_case))
				FILTER (WHERE
					district > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_districts WHERE district = pp.district)) AS districts,
			array_agg(DISTINCT trim(county COLLATE ignore_punct_case))
				FILTER (WHERE
					county > '' AND
					NOT EXISTS (SELECT * FROM eth.pp_counties WHERE county = pp.county)) AS counties
		FROM public.price_paid pp
		WHERE split_part(upper(postcode), ' '::text, 1) = _r.split_part;
		
		INSERT INTO eth.pp_postcodes  (postcode) SELECT UNNEST(postcodes)  FROM pg_temp.lookups WHERE postcodes IS NOT NULL;
		INSERT INTO eth.pp_streets    (street)   SELECT UNNEST(streets)    FROM pg_temp.lookups WHERE streets IS NOT NULL;
		INSERT INTO eth.pp_localities (locality) SELECT UNNEST(localities) FROM pg_temp.lookups WHERE localities IS NOT NULL;
		INSERT INTO eth.pp_towns      (town)     SELECT UNNEST(towns)      FROM pg_temp.lookups WHERE towns IS NOT NULL;
		INSERT INTO eth.pp_districts  (district) SELECT UNNEST(districts)  FROM pg_temp.lookups WHERE districts IS NOT NULL;
		INSERT INTO eth.pp_counties   (county)   SELECT UNNEST(counties)   FROM pg_temp.lookups WHERE counties IS NOT NULL;
		COMMIT;
		
		RAISE NOTICE 'Building addresses in "%" (batch % of % - % rows)', _r.split_part, _i, _total, _cnt;

		TRUNCATE TABLE pg_temp.addresses;
		INSERT INTO pg_temp.addresses (
			ppid, sold_date, price_paid,
			county_id, district_id, town_id, locality_id, street_id, postcode_id,
			poan, soan, uprn, latitude, longitude)
		SELECT
			ppid::uuid, sold_date::date, COALESCE(price_paid::int, 0) AS price_paid,
			county_id, district_id, town_id, locality_id, street_id, postcode_id,
			NULLIF(NULLIF(poan COLLATE ignore_punct_case, 'NaN'), '') AS poan,
			NULLIF(NULLIF(soan COLLATE ignore_punct_case, 'NaN'), '') AS soan,
			uprn::bigint,
			latitude::float,
			longitude::float
		FROM public.price_paid pp
		LEFT JOIN eth.pp_postcodes pc USING (postcode)
		LEFT JOIN eth.pp_streets st USING (street)
		LEFT JOIN eth.pp_localities l USING (locality)
		LEFT JOIN eth.pp_towns t USING (town)
		LEFT JOIN eth.pp_districts d USING (district)
		LEFT JOIN eth.pp_counties c USING (county)
		WHERE split_part(upper(postcode), ' '::text, 1) = _r.split_part
		  AND ppid > '' AND sold_date > '';
		COMMIT;

		CREATE TEMP TABLE new_addresses ON COMMIT DROP AS
		SELECT
			postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id,
			max(uprn) AS uprn, max(latitude) AS latitude, max(longitude) AS longitude
		FROM pg_temp.addresses a
		GROUP BY postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id
		HAVING NOT EXISTS (
			SELECT * FROM eth.pp_addresses
			WHERE (postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id)
				IS NOT DISTINCT FROM
				(a.postcode_id, a.poan, a.soan, a.street_id, a.locality_id, a.town_id, a.district_id, a.county_id)
			);

		IF EXISTS (SELECT * FROM pg_temp.new_addresses) THEN
			RAISE NOTICE 'Inserting new % addresses in "%" (batch % of %)',
				(SELECT COUNT(*) FROM pg_temp.new_addresses), _r.split_part, _i, _total;
			PERFORM setval(pg_get_serial_sequence('eth.pp_addresses', 'address_id'), 
							  _address_id + (SELECT COUNT(*) FROM pg_temp.new_addresses));
			INSERT INTO eth.pp_addresses (
				address_id,
				postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id,
				uprn, latitude, longitude)
			SELECT _address_id + row_number() OVER (ORDER BY postcode_id, street_id, poan, soan),
				postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id,
				uprn, latitude, longitude
			FROM pg_temp.new_addresses;
		END IF;
		COMMIT;

		RAISE NOTICE 'Adding processed price_paid in "%" (batch % of % - % rows)', _r.split_part, _i, _total, _cnt;
		INSERT INTO eth.price_paid_data (address_id, sold_date, price_paid, ppid)
		SELECT DISTINCT ON (address_id, sold_date)
			address_id, sold_date, price_paid, ppid
		FROM pg_temp.addresses t
		JOIN eth.pp_addresses a ON (
			(a.postcode_id, a.poan, a.soan, a.street_id, a.locality_id, a.town_id, a.district_id, a.county_id)
			IS NOT DISTINCT FROM
			(t.postcode_id, t.poan, t.soan, t.street_id, t.locality_id, t.town_id, t.district_id, t.county_id)
			)
		ORDER BY 1, 2, 3 DESC, 4;
		COMMIT;
	END LOOP;

	DROP TABLE pg_temp.addresses;
	
	RETURN;
END;
$$;


CREATE PROCEDURE eth.initialise_lookups()
    LANGUAGE plpgsql
    AS $$
BEGIN
	RAISE NOTICE 'Inserting into postcodes';
	INSERT INTO eth.pp_postcodes (postcode)
	SELECT trim(postcode COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE postcode > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (postcode) DO NOTHING;
	COMMIT;
	
	RAISE NOTICE 'Inserting into streets';
	INSERT INTO eth.pp_streets (street)
	SELECT trim(street COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE trim(street COLLATE ignore_punct_case) > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (street) DO NOTHING;
	COMMIT;

	RAISE NOTICE 'Inserting into localities';
	INSERT INTO eth.pp_localities (locality)
	SELECT trim(locality COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE trim(locality COLLATE ignore_punct_case) > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (locality) DO NOTHING;
	COMMIT;
	
	RAISE NOTICE 'Inserting into towns';
	INSERT INTO eth.pp_towns (town)
	SELECT trim(town COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE trim(town COLLATE ignore_punct_case) > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (town) DO NOTHING;
	COMMIT;
	
	RAISE NOTICE 'Inserting into districts';
	INSERT INTO eth.pp_districts (district)
	SELECT trim(district COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE trim(district COLLATE ignore_punct_case) > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (district) DO NOTHING;
	COMMIT;
	
	RAISE NOTICE 'Inserting into counties';
	INSERT INTO eth.pp_counties (county)
	SELECT trim(county COLLATE ignore_punct_case)
	FROM public.price_paid
	WHERE trim(county COLLATE ignore_punct_case) > ''
	GROUP BY 1 ORDER BY 1
	ON CONFLICT (county) DO NOTHING;
	COMMIT;
END;
$$;


CREATE FUNCTION eth.price_paid_insert_tg() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
	_postcode_id int = postcode_id FROM eth.pp_postcodes WHERE postcode = NEW.postcode;
	_street_id int = street_id FROM eth.pp_streets WHERE street = NEW.street;
	_locality_id smallint = locality_id FROM eth.pp_localities WHERE locality = NEW.locality;
	_town_id smallint = town_id FROM eth.pp_towns WHERE town = NEW.town;
	_district_id smallint = district_id FROM eth.pp_districts WHERE district = NEW.district;
	_county_id smallint = county_id FROM eth.pp_counties WHERE county = NEW.county;
	_preprocess boolean = COALESCE(_postcode_id, _street_id, _locality_id, _town_id, _district_id, _county_id) IS NULL;
	_address_id int;
	_lookup_address boolean = true;
BEGIN
	IF _preprocess THEN
		IF NEW.postcode > '' AND _postcode_id IS NULL THEN
			INSERT INTO eth.pp_postcodes (postcode)
			VALUES (trim(NEW.postcode)) RETURNING postcode_id INTO _postcode_id;
			_lookup_address := false;
		END IF;
		
		IF NEW.street > '' AND _street_id IS NULL THEN
			INSERT INTO eth.pp_streets (street)
			VALUES (trim(NEW.street)) RETURNING street_id INTO _street_id;
			_lookup_address := false;
		END IF;
	
		IF NEW.locality > '' AND _locality_id IS NULL THEN
			INSERT INTO eth.pp_localities (locality)
			VALUES (trim(NEW.locality)) RETURNING locality_id INTO _locality_id;
			_lookup_address := false;
		END IF;
	
		IF NEW.town > '' AND _town_id IS NULL THEN
			INSERT INTO eth.pp_towns (town)
			VALUES (trim(NEW.town)) RETURNING town_id INTO _town_id;
			_lookup_address := false;
		END IF;
		
		IF NEW.district > '' AND _district_id IS NULL THEN
			INSERT INTO eth.pp_districts (district)
			VALUES (trim(NEW.district)) RETURNING district_id INTO _district_id;
			_lookup_address := false;
		END IF;
			
		IF NEW.county > '' AND _county_id IS NULL THEN
			INSERT INTO eth.pp_counties (county)
			VALUES (trim(NEW.county)) RETURNING county_id INTO _county_id;
			_lookup_address := false;
		END IF;
	END IF;

	NEW.poan := NULLIF(NULLIF(trim(NEW.poan COLLATE ignore_punct_case), 'NaN'), '');
	NEW.soan := NULLIF(NULLIF(trim(NEW.soan COLLATE ignore_punct_case), 'NaN'), '');

	IF _lookup_address THEN
		_address_id := address_id
		FROM eth.pp_addresses a
		WHERE
			(postcode_id, a.poan, a.soan, street_id, locality_id, town_id, district_id, county_id) 
			IS NOT DISTINCT FROM
			(_postcode_id, NEW.poan, NEW.soan, _street_id, _locality_id, _town_id, _district_id, _county_id);
	END IF;

	IF _address_id IS NULL THEN
		INSERT INTO eth.pp_addresses (
			postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id,
			uprn, latitude, longitude)
		VALUES (
			_postcode_id, NEW.poan, NEW.soan, _street_id, _locality_id, _town_id, _district_id, _county_id,
			NEW.uprn, NEW.latitude, NEW.longitude)
		RETURNING address_id INTO _address_id;
	END IF;

	INSERT INTO eth.price_paid_data (address_id, sold_date, price_paid, ppid)
	VALUES (_address_id, NEW.sold_date, COALESCE(NEW.price_paid, 0), NEW.ppid)
	ON CONFLICT (address_id, sold_date) DO UPDATE SET
		price_paid = EXCLUDED.price_paid, ppid = EXCLUDED.ppid
		WHERE price_paid_data.price_paid < EXCLUDED.price_paid;

	RETURN NEW;
END
$$;


CREATE PROCEDURE eth.update_price_paid()
    LANGUAGE plpgsql
    AS $$
DECLARE
	_r record;
	_total int;
	_i int = 0;
BEGIN
	RAISE NOTICE 'Determining groups (based on first portion of the postcode)...';
	CREATE TEMP TABLE split_parts AS
		SELECT split_part(upper(postcode), ' '::text, 1)
		FROM (SELECT * FROM public.price_paid LIMIT 10) x
		GROUP BY 1;
	CREATE INDEX ON pg_temp.split_parts (split_part);

	_total := count(*)::int FROM pg_temp.split_parts;
	RAISE NOTICE 'Inserting % groups of price_paid records', _total;
	
	FOR _r IN SELECT * FROM pg_temp.split_parts ORDER BY 1
	LOOP
		_i := _i + 1;
		RAISE NOTICE 'Processing "%" (batch % of % - % rows)', _r.split_part, _i, _total,
			(SELECT COUNT(*) FROM public.price_paid WHERE split_part(upper(postcode), ' '::text, 1) = _r.split_part);
		
		INSERT INTO eth.price_paid
		SELECT DISTINCT ON (ppid::uuid, sold_date::date)
			ppid::uuid, price_paid::int, sold_date::date,
			postcode, poan, soan, street, locality, town, district, county, toolkitid,
			uprn::bigint, latitude::float, longitude::float
		FROM public.price_paid pp
		WHERE split_part(upper(postcode), ' '::text, 1) = _r.split_part
		  AND NOT EXISTS (
		  	SELECT * FROM eth.price_paid_data ppd
			WHERE ppd.ppid = pp.ppid::uuid
			  AND ppd.sold_date = pp.sold_date::date)
		ORDER BY 1, 3, 2 DESC;
		COMMIT;
	END LOOP;
	RETURN;
END;
$$;


CREATE FUNCTION public.address_compare(addr1 text, addr2 text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $$
-- The assumption is that these addresses have parts separated by commas,
-- and that are both in the same postcode.
DECLARE
	_a1 text[];
	_a2 text[];
BEGIN
	-- if strings match, just return true;
	IF addr1 COLLATE num_ignore_punct_case = addr2 COLLATE num_ignore_punct_case THEN
		RETURN true;
	END IF;
	-- split into portions separated by commas (ignoring leading and trailing spaces)
	SELECT regexp_split_to_array(trim(addr1 COLLATE "default"), ' *, *'),
		regexp_split_to_array(trim(addr2 COLLATE "default"), ' *, *')
		INTO _a1, _a2;
	-- if both first portions have no numbers and they both match
	IF _a1[1] COLLATE "default" !~ '^[0-9]' AND _a2[1] COLLATE "default" !~ '^[0-9]' THEN
		-- remove the non-numeric first portion, to compare the rest
		IF _a1[1] COLLATE ignore_punct_case = _a1[2] COLLATE ignore_punct_case THEN
			_a1 := _a1[2:9];
			_a2 := _a2[2:9];
		ELSE -- otherwise they won't match anyway, so return false
			RETURN false;
		END IF;
	END IF;
	-- merge first and second portion of both addresses whenever the first portion has no letters
	IF _a1[1] COLLATE "default" !~* '[a-z]' THEN
		_a1 := concat_ws(' ', _a1[1], _a1[2]) || COALESCE(_a1[3:9], '{}');
	END IF;
	IF _a2[1] COLLATE "default" !~* '[a-z]' THEN
		_a2 := concat_ws(' ', _a2[1], _a2[2]) || COALESCE(_a2[3:9], '{}');
	END IF;
	-- if there is only one portion in either address,
	--   just compare that portion provided that the addresses compared are at least 8 characters
	IF CARDINALITY(_a1) = 1 OR CARDINALITY(_a2) = 1 THEN
		RETURN _a1[1] COLLATE num_ignore_punct_case = _a2[1] COLLATE num_ignore_punct_case AND
			LENGTH(_a1[1]) > 10;
	ELSE -- otherwise compare both first and second portions (ignore the rest)
		RETURN _a1[1] COLLATE num_ignore_punct_case = _a2[1] COLLATE num_ignore_punct_case
		   AND LEAST(LENGTH(_a1[2]), LENGTH(_a2[2])) > 2
		   AND LEFT(_a1[2], LEAST(LENGTH(_a1[2]), LENGTH(_a2[2]))) COLLATE num_ignore_punct_case =
			   LEFT(_a2[2], LEAST(LENGTH(_a1[2]), LENGTH(_a2[2]))) COLLATE num_ignore_punct_case;
	END IF;
	RETURN false;
END;
$$;


CREATE PROCEDURE public.clean_brand_aliases()
    LANGUAGE plpgsql
    AS $_$
BEGIN
    -- 1. Initial Data Insertion
    TRUNCATE TABLE brand_aliases; -- Clear any existing data
    INSERT INTO brand_aliases (original_name, cleaned_name)
    SELECT DISTINCT brandname, brandname -- Use original brandname as initial cleaned_name
    FROM listing_scrapper_data;

    -- 2. Remove text after double space or space-dash-space
    UPDATE brand_aliases
    SET cleaned_name = TRIM(
        REGEXP_REPLACE(cleaned_name, '\s{2,}.*| - .*', '', 'g')
    )
    WHERE cleaned_name ~ '\s{2,}| - ';

    -- 3. Replace "&" with "and"
    UPDATE brand_aliases
    SET cleaned_name = REGEXP_REPLACE(cleaned_name, '\s*&\s*', ' and ', 'g')
    WHERE cleaned_name LIKE '%&%';

    -- 4. Remove "ltd", "llp", "limited", "ltd."
    UPDATE brand_aliases
    SET cleaned_name = LEFT(cleaned_name, LENGTH(cleaned_name) - 4)
    WHERE cleaned_name ILIKE '% ltd'
       OR cleaned_name ILIKE '% llp';

    UPDATE brand_aliases
    SET cleaned_name = LEFT(cleaned_name, LENGTH(cleaned_name) - 8)
    WHERE cleaned_name ILIKE '% limited';

    UPDATE brand_aliases
    SET cleaned_name = LEFT(cleaned_name, LENGTH(cleaned_name) - 5)
    WHERE cleaned_name ILIKE '% ltd.';

    -- 5. Remove text after ", powered by"
    UPDATE brand_aliases
    SET cleaned_name = TRIM(
        REGEXP_REPLACE(cleaned_name, '\s*,\s*powered by.*$', '', 'gi')
    )
    WHERE cleaned_name ILIKE '%, powered by%';

    -- 6. Convert to lowercase
    UPDATE brand_aliases
    SET cleaned_name = LOWER(cleaned_name);

    -- 7. Remove brackets, commas, and full stops (except in TLDs)
    UPDATE brand_aliases
    SET cleaned_name = REGEXP_REPLACE(
        cleaned_name,
        '[\(\),.]' ||
        '(?!(?:co\.uk|com|\.net|\.org|\.info|\.me|\.uk|\.co)\b)',
        '',
        'g'
    );

END;
$_$;


CREATE FUNCTION public.clean_listing_brandname() RETURNS void
    LANGUAGE plpgsql
    AS $_$
BEGIN
    UPDATE listing_scrapper_data
    SET brandname = 
        regexp_replace( -- Remove " - Something" pattern
            regexp_replace(brandname, ' - .*$', '', 'g'),
            '(-|\s)and(-|\s)', ' and ', 'gi' -- Fix missing "and"
        );
END;
$_$;


CREATE PROCEDURE public.create_price_paid_uprn_matches(IN p_truncate_existing boolean DEFAULT false)
    LANGUAGE plpgsql
    AS $$ -- Code taken from the script create_price_paid_uprn_matches.sh
-- This process assigns matching uprn values (i.e. unique addresses) to ppid values from price_paid.
-- It also identifies whether the uprn is one of several possible ones, and if the ppid is one of several possible ones.
DECLARE
	_pc record;
	_count bigint;
BEGIN
	RAISE NOTICE 'Remember to update table uprn_addresses by running procedure update_uprn_addresses() first!';

	--DROP TABLE IF EXISTS price_paid_uprn_matches;
	IF to_regclass('public.price_paid_uprn_matches') IS NULL THEN
		CREATE TABLE IF NOT EXISTS price_paid_uprn_matches (
	        ppid uuid NOT NULL PRIMARY KEY,
	        uprn bigint,
	        matches smallint,
	        errors smallint
		);
	ELSIF p_truncate_existing THEN
		RAISE NOTICE 'Truncating table price_paid_uprn_matches.';
		TRUNCATE TABLE price_paid_uprn_matches;
	END IF;
	
	RAISE NOTICE 'Creating list of postcode first portions (up to the space) for price_paid records without UPRN...';
	IF to_regclass('pg_temp.postcodes_first_half') IS NOT NULL THEN
		DROP TABLE pg_temp.postcodes_first_half;
	END IF;
	CREATE TEMP TABLE postcodes_first_half AS
	SELECT split_part(upper(pp.postcode), ' ', 1) AS pp
	FROM price_paid pp
	WHERE pp.postcode <> 'NaN' AND uprn IS NULL
	GROUP BY 1;
	GET DIAGNOSTICS _count = ROW_COUNT;
	
	FOR _pc IN
		SELECT row_number() OVER (ORDER BY pp) AS rn, pp
		FROM pg_temp.postcodes_first_half
		ORDER BY pp
	LOOP
		RAISE NOTICE 'Finding matches in uprn_addresses for price_paid with postcode first portion = % (%/%)',
			_pc.pp, _pc.rn, _count;
		WITH
			pp AS (
				SELECT DISTINCT ON (pp.ppid::uuid::text)
					pp.ppid::uuid, pp.postcode, pp.soan, pp.poan, pp.street,
					pp.locality, pp.town, pp.district, pp.county
				FROM price_paid pp
				WHERE split_part(upper(pp.postcode), ' ', 1) = _pc.pp
				  AND uprn IS NULL
				  AND NOT EXISTS (SELECT * FROM price_paid_uprn_matches m WHERE m.ppid = pp.ppid::uuid)
				ORDER BY pp.ppid::uuid::text
				)
		INSERT INTO price_paid_uprn_matches AS i (ppid, uprn, matches, errors)
		SELECT pp.ppid, MIN(ua.uprn) AS uprn, COUNT(DISTINCT ua.uprn) AS matches, NULL
		FROM pp
		LEFT JOIN uprn_addresses ua ON (
			ua.postcode = pp.postcode AND
			ua.address =ANY(potential_addresses(
				pp.poan, pp.soan, pp.street, pp.locality, pp.town, pp.district, pp.county)))
		WHERE split_part(upper(pp.postcode), ' ', 1) = _pc.pp
		GROUP BY 1 ORDER BY 1
		ON CONFLICT (ppid) DO UPDATE SET errors = COALESCE(i.errors, 0) + 1;

		COMMIT;
	END LOOP;

	-- Errors (multiple addresses for the same ppid) can be addressed prioritising:
	---- the one with an address match
	---- the one with locality <> 'NaN'
	---- the one with a soan <> 'NaN'
	---- the one with a poan that is numeric instead of text

	DROP TABLE pg_temp.postcodes_first_half;
END
$$;


CREATE PROCEDURE public.listing_update_lease_years()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET yearsremainingonlease = (regexp_matches(text_description, '\y(\d{2,3})\s+years?\s+remaining', 'g'))[1]::INT
    WHERE 
        propertytype = 'flat'
        AND (yearsremainingonlease IS NULL OR yearsremainingonlease = '')
        AND text_description ~ '\y\d{2,3}\s+years?\s+remaining';
END $$;


CREATE FUNCTION public.potential_addresses(poan text, soan text, street text, locality text, town text, district text, county text) RETURNS text[]
    LANGUAGE plpgsql
    AS $$
DECLARE
	_ret text[];
BEGIN
	SELECT
		NULLIF(soan, 'NaN'), NULLIF(poan, 'NaN'), NULLIF(street, 'NaN'), NULLIF(locality, 'NaN'),
		NULLIF(town, 'NaN'), NULLIF(district, 'NaN'), NULLIF(county, 'NaN')
	INTO soan, poan, street, locality, town, district, county;
	_ret := ARRAY[
		concat_ws(' ', soan, poan, street),
		concat_ws(' ', soan, poan, street, locality),
		concat_ws(' ', soan, poan, street, locality, town),
		concat_ws(' ', soan, poan, street, locality, town, district),
		concat_ws(' ', soan, poan, street, locality, town, district, county),
		concat_ws(' ', soan, poan, street, town),
		concat_ws(' ', soan, poan, street, town, district),
		concat_ws(' ', soan, poan, street, town, district, county),
		concat_ws(' ', soan, poan, street, district),
		concat_ws(' ', soan, poan, street, district, county),
		concat_ws(' ', soan, poan, street, county)];
	IF soan ILIKE 'FLAT %' THEN
		_ret := _ret || potential_addresses(
			substring(soan FROM 6), poan, street, locality, town, district, county);
	END IF;
	RETURN _ret;
END;
$$;


CREATE PROCEDURE public.update_brand_aliases()
    LANGUAGE plpgsql
    AS $_$
BEGIN
    -- Insert new brand names into brand_aliases
    INSERT INTO brand_aliases (original_name, cleaned_name)
    SELECT DISTINCT 
        brandname AS original_name,
        TRIM( -- Final trim
            REGEXP_REPLACE( -- 5. Remove anything after double spaces
                REGEXP_REPLACE( -- 4. Location removal
                    REGEXP_REPLACE( -- 3. Remove common suffixes
                        REGEXP_REPLACE( -- 2. Normalize delimiters
                            LOWER(brandname), -- 1. Lowercase
                            '(\s*[-,]\s*|,)',
                            ' ',
                            'g'
                        ),
                        '(\s+Ltd|\s+Limited|\s+LLP|\s+LLC|\s+INC\.?|\s+INC)$',
                        '',
                        'gi'
                    ),
                    '(?i)(\s+|-|,)\s*' || location || '\s*$' ||
                    CASE
                        WHEN geotype IN ('City', 'Town', 'Village', 'County') THEN ''
                        ELSE ''
                    END,
                    '',
                    'g'
                ),
                '  .*$',  -- Remove anything after first double space
                '',
                'g'
            )
        ) AS cleaned_name
    FROM listing_scrapper_data
    CROSS JOIN uk_locations
    WHERE brandname LIKE '%' || location || '%'
    ON CONFLICT (original_name) DO NOTHING;

    -- Update existing records to remove anything after double spaces
    UPDATE brand_aliases
    SET cleaned_name = LEFT(cleaned_name, POSITION('  ' IN cleaned_name) - 1)
    WHERE cleaned_name LIKE '%  %';

END;
$_$;


CREATE PROCEDURE public.update_brand_names()
    LANGUAGE plpgsql
    AS $$
DECLARE
    batch_size INT := 10000; -- Adjust for performance
    rows_updated INT := 1;
BEGIN
    -- 1. Back up new brand names before updating
    INSERT INTO brand_aliases_backup (toolkitid, original_name)
    SELECT ls.toolkitid, ls.brandname
    FROM listing_scrapper_data ls
    JOIN propertytool pt ON ls.toolkitid = pt.toolkitid
    WHERE pt.created_at >= NOW() - INTERVAL '1 day' 
      AND pt.created_at < NOW() - INTERVAL '3 hours' -- Ensures we don't miss new cleaned names
    ON CONFLICT (toolkitid) DO NOTHING; -- Avoid duplicates

    -- 2. Batch update new brand names using cleaned_name
    WHILE rows_updated > 0 LOOP
        UPDATE listing_scrapper_data ls
        SET brandname = ba.cleaned_name
        FROM brand_aliases ba
        WHERE ls.brandname = ba.original_name
        AND ls.toolkitid IN (
            SELECT toolkitid 
            FROM propertytool 
            WHERE created_at >= NOW() - INTERVAL '1 day' 
              AND created_at < NOW() - INTERVAL '3 hours' -- Syncs with 3 AM update
            LIMIT batch_size
        );

        -- Get number of updated rows
        GET DIAGNOSTICS rows_updated = ROW_COUNT;

        -- Commit after each batch
        COMMIT;
    END LOOP;
END;
$$;


CREATE PROCEDURE public.update_flat_property_types()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET propertytype = 'flat'
    WHERE (propertysubtype = 'apartment' OR propertysubtype = 'flat' OR propertysubtype = 'maisonette') AND propertytype IS NULL;
END;
$$;


CREATE PROCEDURE public.update_hmo_property_types()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET propertysubtype = 'hmo',
        propertytype = 'house'
    WHERE propertysubtype IN ('house-share', 'house-of-multiple-occupation');
END;
$$;


CREATE PROCEDURE public.update_house_property_types()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET propertytype = 'house'
    WHERE propertysubtype ILIKE 'detached'
       OR propertysubtype ILIKE 'semi-detached%'
       OR propertysubtype ILIKE 'end-terrace%'
       OR propertysubtype ILIKE 'terraced'
       OR propertysubtype ILIKE 'semi-detached-bungalow%'
       OR propertysubtype ILIKE 'bungalow'
       OR propertysubtype ILIKE 'barn-conversion%'
       OR propertysubtype ILIKE 'cottage'
       OR propertysubtype ILIKE 'barn'
       AND propertytype IS NULL;
END;
$$;


CREATE PROCEDURE public.update_lease_years_remaining()
    LANGUAGE plpgsql
    AS $$
DECLARE
    current_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
BEGIN
    UPDATE listing_scrapper_data
    SET yearsremainingonlease =
        CASE
            WHEN text_description ~* 'years? remaining:?\s*(\d+)' THEN
                (REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'))::INT
            WHEN text_description ~* '\((\d+)\s+years?\s+remaining\)' THEN
                (REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'))::INT
            WHEN text_description ~* 'leasehold.*granted in.*\s(\d{4})' THEN
                (REGEXP_REPLACE(REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'),'(\d{4})', ''))::INT - (current_year - (REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'))::INT)
            WHEN text_description ~* 'LEASE REMAINING\s+(\d+)\s+years from.*\s(\d{4})' THEN
                (REGEXP_REPLACE(REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'),'(\d{4})', ''))::INT - (current_year - (REGEXP_REPLACE(text_description, '[^0-9]+', '', 'g'))::INT)
            ELSE
                NULL
        END
    WHERE tenuretype = 'leasehold' AND yearsremainingonlease IS NULL;
END;
$$;


CREATE PROCEDURE public.update_leasehold_tenure()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET tenuretype = 'leasehold'
    WHERE (propertytype = 'flat' OR propertysubtype = 'flat')
      AND tenuretype IS NULL
      AND text_description ILIKE '%leasehold%';
END;
$$;


CREATE PROCEDURE public.update_listing_property_types_daily()
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- 1. If propertytype = studio, update to propertysubtype = studio and propertytype = flat
  UPDATE listing_scrapper_data
  SET
    propertysubtype = 'studio',
    propertytype = 'flat'
  WHERE
    propertytype = 'studio';

  -- 2. If propertytype = flat, text_description contains "studio", and propertytool.bedrooms = 1, update propertysubtype to studio
  UPDATE listing_scrapper_data lsd
  SET
    propertysubtype = 'studio'
  FROM
    propertytool pt
  WHERE
    lsd.toolkitid = pt.toolkitid
    AND lsd.propertytype = 'flat'
    AND lsd.text_description ILIKE '%studio%'
    AND pt.bedrooms = 1;

  -- 3. ensures property types are studios or have 0 bedrooms where agents mislead / advertise as 1 beds.
  UPDATE propertytool pt
  SET bedrooms = 0
  WHERE EXISTS (
    SELECT 1
    FROM listing_scrapper_data lsd
    WHERE lsd.toolkitid = pt.toolkitid
      AND lsd.propertysubtype = 'studio'
      AND pt.bedrooms = 1
  );

  -- 4. If propertysubtype = apartment OR FLAT or flat, update propertysubtype to flat
  UPDATE listing_scrapper_data
  SET
    propertysubtype = 'flat'
  WHERE
    propertysubtype ILIKE 'apartment'
    OR propertysubtype ILIKE 'FLAT'
    OR propertysubtype ILIKE 'flat';

  -- 5. If propertysubtype = block-of-apartments, update propertytype to flat
  UPDATE listing_scrapper_data
  SET
    propertytype = 'flat'
  WHERE
    propertysubtype = 'block-of-apartments';

  -- 6. If propertytype = Flats / Apartments, update propertytype to flat
UPDATE listing_scrapper_data
  SET
    propertytype = 'flat'
  WHERE
    propertytype = 'Flats / Apartments';

  COMMIT;
END;
$$;


CREATE PROCEDURE public.update_listing_scrapper_data(IN p_batch_size integer DEFAULT 1000)
    LANGUAGE plpgsql
    AS $$
DECLARE
	_r record;
	_count bigint;
	_total bigint;
BEGIN
	p_batch_size := COALESCE(NULLIF(GREATEST(p_batch_size, 0), 0), 1000);
	RAISE NOTICE 'Executing update_listing_scrapper_data(%)', p_batch_size;
	
	RAISE NOTICE 'Updating first by price + year...';
	WITH
	    scrapper AS (
	        SELECT id,
	            postcode,
	            transactionhistoryprice AS price_paid,
	            transactionhistoryyear AS year
	        FROM listing_scrapper_data
	        WHERE (uprn IS NULL OR uprn = 0 OR NULLIF(TRIM(address), '') IS NULL)
	          AND postcode ~* '[a-z]'
	          AND transactionhistoryprice > 0
	          AND transactionhistoryyear > 0
	        ),
	    scrapper_postcodes AS (
	        SELECT DISTINCT postcode FROM scrapper
	        ),
	    price_paid AS (
	        SELECT ppid, uprn, postcode,
	            concat_ws(', ', 
					concat_ws(' ', 
						concat_ws(', ', NULLIF(soan, 'NaN'), NULLIF(poan, 'NaN')),
						NULLIF(street, 'NaN')),
					NULLIF(locality, 'NaN'), NULLIF(town, 'NaN'), NULLIF(district, 'NaN'), NULLIF(county, 'NaN')) AS address,
	            price_paid AS price_paid,
	            date_part('year', sold_date::timestamp) AS year
	        FROM price_paid
	        WHERE postcode IN (SELECT postcode FROM scrapper_postcodes)
	        ),
	    matches AS (
	        SELECT *
	        FROM scrapper s
	        JOIN price_paid pp USING (postcode, price_paid, year)
	        ),
	    stats AS (
	        SELECT count(distinct id) AS distinct_scrapper_id,
	            count(DISTINCT ppid) AS distinct_ppid,
	            count(NULLIF(uprn, 0)) AS pp_with_uprn,
	            count(DISTINCT NULLIF(uprn, 0)) AS distinct_pp_with_uprn,
	            count(NULLIF(trim(address), '')) AS pp_with_address,
	            count(DISTINCT NULLIF(trim(address), '')) AS distinct_pp_with_address,
	            count(DISTINCT NULLIF(trim(address), '')||year::text) AS distinct_pp_with_address_year,
	            count(*) AS total_rows_joined
	        FROM matches
	        ),
	    address_year_price_matches AS (
	        SELECT DISTINCT id, address, postcode, price_paid, year, uprn
	        FROM matches
	        )
	UPDATE listing_scrapper_data AS u SET
		address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
		uprn = COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0)
	FROM address_year_price_matches m
	WHERE u.id = m.id;
	GET DIAGNOSTICS _count = ROW_COUNT;
	IF _count > 0 THEN
		RAISE NOTICE '  >> updated % listing_scrapper_data rows by price + year.', _count;
		_total := _total + _count;
	END IF;
	COMMIT;
	
	RAISE NOTICE 'Obtaining list of postcodes to split work, in batches of % postcodes...', p_batch_size;

	IF to_regclass('pg_temp.my_postcodes') IS NOT NULL THEN
		DROP TABLE IF EXISTS pg_temp.my_postcodes;
	END IF;

	CREATE TEMP TABLE my_postcodes AS
	SELECT row_number / p_batch_size AS batch,
		min(postcode) AS pc_min, max(postcode) AS pc_max, count(DISTINCT postcode) AS pc_count
	FROM (
		SELECT row_number() OVER (ORDER BY postcode) - 1 AS row_number, postcode
		FROM (
			SELECT DISTINCT postcode
			FROM listing_scrapper_data
	        WHERE (uprn IS NULL OR uprn = 0 OR NULLIF(TRIM(address), '') IS NULL)
			  AND postcode ~* '[a-z]'
			  AND (
			  	(length(trim(epc_date)) >= 8 AND epc_rating ~* '^[a-z]') OR
				(epc_current_value > 0 AND epc_potential_value > 0))
			) x
		) y
	GROUP BY 1;
	COMMIT;

	RAISE NOTICE 'Total number of postcodes = %, total number of batches = %.',
		(SELECT SUM(pc_count) FROM pg_temp.my_postcodes),
		(SELECT COUNT(*) FROM pg_temp.my_postcodes);

	FOR _r IN SELECT * FROM my_postcodes ORDER BY batch
	LOOP
		RAISE NOTICE 'Processing scrapper from % to % (% postcodes)...', _r.pc_min, _r.pc_max, _r.pc_count;

		WITH
		    scrapper AS (
		        SELECT id,
		            postcode,
		            transactionhistoryprice AS price_paid,
		            transactionhistoryyear AS year
		        FROM listing_scrapper_data
    	        WHERE (uprn IS NULL OR uprn = 0 OR NULLIF(TRIM(address), '') IS NULL)
		          AND postcode BETWEEN _r.pc_min AND _r.pc_max
		          AND transactionhistoryprice > 0
		          AND transactionhistoryyear > 0
		        ),
		    scrapper_postcodes AS (
		        SELECT DISTINCT postcode FROM scrapper
		        ),
		    price_paid AS (
		        SELECT ppid, uprn, postcode,
		            concat_ws(', ', 
						concat_ws(' ', 
							concat_ws(', ', NULLIF(soan, 'NaN'), NULLIF(poan, 'NaN')),
							NULLIF(street, 'NaN')),
						NULLIF(locality, 'NaN'), NULLIF(town, 'NaN'), NULLIF(district, 'NaN'), NULLIF(county, 'NaN')) AS address,
		            price_paid AS price_paid,
		            date_part('year', sold_date::timestamp) AS year
		        FROM price_paid
		        WHERE postcode IN (SELECT postcode FROM scrapper_postcodes)
		        ),
		    matches AS (
		        SELECT *
		        FROM scrapper s
		        JOIN price_paid pp USING (postcode, price_paid, year)
		        ),
		    stats AS (
		        SELECT count(distinct id) AS distinct_scrapper_id,
		            count(DISTINCT ppid) AS distinct_ppid,
		            count(NULLIF(uprn, 0)) AS pp_with_uprn,
		            count(DISTINCT NULLIF(uprn, 0)) AS distinct_pp_with_uprn,
		            count(NULLIF(trim(address), '')) AS pp_with_address,
		            count(DISTINCT NULLIF(trim(address), '')) AS distinct_pp_with_address,
		            count(DISTINCT NULLIF(trim(address), '')||year::text) AS distinct_pp_with_address_year,
		            count(*) AS total_rows_joined
		        FROM matches
		        ),
		    address_year_price_matches AS (
		        SELECT DISTINCT id, address, postcode, price_paid, year, uprn
		        FROM matches
		        )
		UPDATE listing_scrapper_data AS u SET
			address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
			uprn = COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0)
		FROM address_year_price_matches m
		WHERE u.id = m.id;
		GET DIAGNOSTICS _count = ROW_COUNT;
		IF _count > 0 THEN
			RAISE NOTICE '  >> updated % listing_scrapper_data rows by price + year.', _count;
			_total := _total + _count;
		END IF;
		
		WITH
		    scrapper AS (
		        SELECT id,
		            postcode,
		            trim(epc_date)::date AS epc_date,
		            trim(epc_rating) AS epc_rating
		        FROM listing_scrapper_data
    	        WHERE (uprn IS NULL OR uprn = 0 OR NULLIF(TRIM(address), '') IS NULL)
		          AND postcode BETWEEN _r.pc_min AND _r.pc_max
		          AND length(trim(epc_date)) >= 8
		          AND epc_rating ~* '^[a-z]'
		        ),
		    scrapper_postcodes AS (
		        SELECT DISTINCT postcode FROM scrapper
		        ),
			certs AS (
		        SELECT lmk_key,
		            c.postcode, c.address, u.uprn,
		            lodgement_date AS epc_date,
		            trim(current_energy_rating) AS epc_rating
		        FROM certificates_depc c
				LEFT JOIN uprn_addresses u USING (postcode, address)
		        WHERE postcode BETWEEN _r.pc_min AND _r.pc_max
				  AND TRIM(address) > ''
		          AND lodgement_date IS NOT NULL
		          AND TRIM(current_energy_rating) > ''
				),
		    matches AS (
		        SELECT *
		        FROM scrapper s
		        JOIN certs c USING (postcode, epc_date, epc_rating)
		        ),
		    epc_date_and_rating_matches AS (
		        SELECT id, postcode, epc_date, epc_rating, address, uprn
		        FROM matches
				WHERE id NOT IN (SELECT id FROM matches GROUP BY id HAVING count(*) > 1)
		        )
		UPDATE listing_scrapper_data AS u SET
			address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
			uprn = COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0)
		FROM epc_date_and_rating_matches m -- */
		WHERE u.id = m.id
		  AND (u.address IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.address), ''), m.address)
		       OR u.uprn IS DISTINCT FROM COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0));
		GET DIAGNOSTICS _count = ROW_COUNT;
		IF _count > 0 THEN
			RAISE NOTICE '  >> updated % listing_scrapper_data rows by epc date + rating.', _count;
			_total := _total + _count;
		END IF;

		WITH
		    scrapper AS (
		        SELECT id,
		            postcode,
		            epc_current_value AS current_value,
		            epc_potential_value AS potential_value
		        FROM listing_scrapper_data
    	        WHERE (uprn IS NULL OR uprn = 0 OR NULLIF(TRIM(address), '') IS NULL)
		          AND postcode BETWEEN _r.pc_min AND _r.pc_max
				  AND epc_current_value > 0
				  AND epc_potential_value > 0
		        ),
		    scrapper_postcodes AS (
		        SELECT DISTINCT postcode FROM scrapper
		        ),
			certs AS (
		        SELECT lmk_key,
		            c.postcode, c.address, u.uprn,
		            c.current_energy_efficiency AS current_value,
		            c.potential_energy_efficiency AS potential_value
		        FROM certificates_depc c
				LEFT JOIN uprn_addresses u USING (postcode, address)
		        WHERE postcode BETWEEN _r.pc_min AND _r.pc_max
				  AND TRIM(address) > ''
		          AND current_energy_efficiency > 0
		          AND potential_energy_efficiency > 0
				),
		    matches AS (
		        SELECT *
		        FROM scrapper s
		        JOIN certs c USING (postcode, current_value, potential_value)
		        ),
		    epc_current_and_potential_matches AS (
		        SELECT id, postcode, current_value, potential_value, address, uprn
		        FROM matches
				WHERE id NOT IN (SELECT id FROM matches GROUP BY id HAVING count(*) > 1)
		        )
		UPDATE listing_scrapper_data AS u SET
			address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
			uprn = COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0)
		FROM epc_current_and_potential_matches m -- */
		WHERE u.id = m.id
		  AND (u.address IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.address), ''), m.address)
		       OR u.uprn IS DISTINCT FROM COALESCE(NULLIF(u.uprn, 0), m.uprn::bigint, 0));
		GET DIAGNOSTICS _count = ROW_COUNT;
		IF _count > 0 THEN
			RAISE NOTICE '  >> updated % listing_scrapper_data rows by epc current and potential energy value.', _count;
			_total := _total + _count;
		END IF;
		
		COMMIT;
	END LOOP;
	DROP TABLE pg_temp.my_postcodes;
	RAISE NOTICE 'Finished, having updated a total of % listing_scrapper_data rows', _total;
END;
$$;


CREATE PROCEDURE public.update_listing_scrapper_data_by_current_potential_rating(IN p_batch_size integer DEFAULT 1000)
    LANGUAGE plpgsql
    AS $$
DECLARE
	_r record;
	_count bigint;
	_total bigint = 0;
BEGIN
	p_batch_size := COALESCE(NULLIF(GREATEST(p_batch_size, 0), 0), 1000);
	RAISE NOTICE 'Executing update_listing_scrapper_data_by_current_potential_rating(%)', p_batch_size;
	RAISE NOTICE 'Obtaining list of postcodes to split work, in batches of % postcodes...', p_batch_size;

	IF to_regclass('pg_temp.my_postcodes') IS NOT NULL THEN
		DROP TABLE IF EXISTS pg_temp.my_postcodes;
	END IF;

	CREATE TEMP TABLE my_postcodes AS
	SELECT row_number / p_batch_size AS batch,
		min(postcode) AS pc_min, max(postcode) AS pc_max, count(DISTINCT postcode) AS pc_count
	FROM (
		SELECT row_number() OVER (ORDER BY postcode) - 1 AS row_number, postcode
		FROM (
			SELECT DISTINCT analyticsinfo_analyticsproperty_postcode AS postcode
			FROM listing_scrapper_data
			WHERE (NULLIF(TRIM(urpn), '') IS NULL OR NULLIF(TRIM(address), '') IS NULL)
			  AND analyticsinfo_analyticsproperty_postcode ~* '[a-z]'
			  AND epc_current_value > 0
			  AND epc_potential_value > 0
			) x
		) y
	GROUP BY 1;

	RAISE NOTICE 'Total number of postcodes = %, total number of batches = %.',
		(SELECT SUM(pc_count) FROM pg_temp.my_postcodes),
		(SELECT COUNT(*) FROM pg_temp.my_postcodes);

	FOR _r IN SELECT * FROM my_postcodes ORDER BY batch
	LOOP
		RAISE NOTICE 'Processing scrapper from % to % (% postcodes)...',
			_r.pc_min, _r.pc_max, _r.pc_count;
		WITH
		    scrapper AS (
		        SELECT id,
		            analyticsinfo_analyticsproperty_postcode AS postcode,
		            epc_current_value AS current_value,
		            epc_potential_value AS potential_value
		        FROM listing_scrapper_data
		        WHERE (NULLIF(TRIM(urpn), '') IS NULL OR NULLIF(TRIM(address), '') IS NULL)
		          AND analyticsinfo_analyticsproperty_postcode BETWEEN _r.pc_min AND _r.pc_max -- ~* '[a-z]'
				  AND epc_current_value > 0
				  AND epc_potential_value > 0
		        ),
		    scrapper_postcodes AS (
		        SELECT DISTINCT postcode FROM scrapper
		        ),
			certs AS (
		        SELECT lmk_key,
		            c.postcode, c.address, u.uprn,
		            c.current_energy_efficiency AS current_value,
		            c.potential_energy_efficiency AS potential_value
		        FROM certificates_depc c
				LEFT JOIN uprn_addresses u USING (postcode, address)
		        WHERE postcode BETWEEN _r.pc_min AND _r.pc_max
				  AND TRIM(address) > ''
		          AND current_energy_efficiency > 0
		          AND potential_energy_efficiency > 0
				),
		    matches AS (
		        SELECT *
		        FROM scrapper s
		        JOIN certs c USING (postcode, current_value, potential_value)
		        ),
		    /*stats AS (
		        SELECT count(distinct id) AS distinct_scrapper_id,
		            count(DISTINCT lmk_key) AS distinct_lmk_key,
		            count(NULLIF(uprn, 0)) AS cd_with_uprn,
		            count(DISTINCT NULLIF(uprn, 0)) AS distinct_cd_with_uprn,
		            count(NULLIF(trim(address), '')) AS cd_with_address,
		            count(DISTINCT NULLIF(trim(address), '')) AS distinct_cd_with_address,
		            count(DISTINCT NULLIF(trim(address), '')||current_value::text) AS distinct_cd_with_current_and_potential_rating,
		            count(*) AS total_rows_joined
		        FROM matches
		        ), --*/
		    epc_date_and_rating_matches AS (
		        SELECT id, postcode, epc_date, epc_rating, address, uprn
		        FROM matches
				WHERE id NOT IN (SELECT id FROM matches GROUP BY id HAVING count(*) > 1)
		        )
		UPDATE listing_scrapper_data AS u SET
			address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
			urpn = COALESCE(NULLIF(TRIM(u.urpn), ''), m.uprn::text)
		FROM epc_date_and_rating_matches m -- */
		WHERE u.id = m.id
		  AND (u.address IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.address), ''), m.address)
		       OR u.urpn IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.urpn), ''), m.uprn::text));
		GET DIAGNOSTICS _count = ROW_COUNT;
		RAISE NOTICE '  >> updated % listing_scrapper_data rows.', _count;
		_total := _total + COALESCE(_count, 0);

		COMMIT;
	END LOOP;
	DROP TABLE pg_temp.my_postcodes;
	RAISE NOTICE 'Finished, having updated a total of % listing_scrapper_data rows', _total;
END;
$$;


CREATE PROCEDURE public.update_listing_scrapper_data_by_epc_date_rating(IN p_batch_size integer DEFAULT 1000)
    LANGUAGE plpgsql
    AS $$
DECLARE
	_r record;
	_count bigint;
	_total bigint = 0;
BEGIN
	p_batch_size := COALESCE(NULLIF(GREATEST(p_batch_size, 0), 0), 1000);
	RAISE NOTICE 'Executing update_listing_scrapper_data_by_epc_date_rating(%)', p_batch_size;
	RAISE NOTICE 'Obtaining list of postcodes to split work, in batches of % postcodes...', p_batch_size;

	IF to_regclass('pg_temp.my_postcodes') IS NOT NULL THEN
		DROP TABLE IF EXISTS pg_temp.my_postcodes;
	END IF;

	CREATE TEMP TABLE my_postcodes AS
	SELECT row_number / p_batch_size AS batch,
		min(postcode) AS pc_min, max(postcode) AS pc_max, count(DISTINCT postcode) AS pc_count
	FROM (
		SELECT row_number() OVER (ORDER BY postcode) - 1 AS row_number, postcode
		FROM (
			SELECT DISTINCT analyticsinfo_analyticsproperty_postcode AS postcode
			-- SELECT COUNT(*) -- 521733 down to 186831
			-- SELECT COUNT(DISTINCT analyticsinfo_analyticsproperty_postcode) -- 330163
			FROM listing_scrapper_data
			WHERE (NULLIF(TRIM(urpn), '') IS NULL OR NULLIF(TRIM(address), '') IS NULL)
			  AND analyticsinfo_analyticsproperty_postcode ~* '[a-z]'
			  AND length(trim(epc_date)) >= 8
			  AND trim(epc_rating) > ''
			) x
		) y
	GROUP BY 1;

	RAISE NOTICE 'Total number of postcodes = %, total number of batches = %.',
		(SELECT SUM(pc_count) FROM pg_temp.my_postcodes),
		(SELECT COUNT(*) FROM pg_temp.my_postcodes);

	FOR _r IN SELECT * FROM my_postcodes ORDER BY batch
	LOOP
		RAISE NOTICE 'Processing scrapper from % to % (% postcodes)...',
			_r.pc_min, _r.pc_max, _r.pc_count;
		WITH
		    scrapper AS (
		        SELECT id,
		            analyticsinfo_analyticsproperty_postcode AS postcode,
		            trim(epc_date)::date AS epc_date,
		            trim(epc_rating) AS epc_rating
		        FROM listing_scrapper_data
		        WHERE (NULLIF(TRIM(urpn), '') IS NULL OR NULLIF(TRIM(address), '') IS NULL)
		          AND analyticsinfo_analyticsproperty_postcode BETWEEN _r.pc_min AND _r.pc_max -- ~* '[a-z]'
		          AND length(trim(epc_date)) >= 8
		          AND trim(epc_rating) > ''
		        ),
		    scrapper_postcodes AS (
		        SELECT DISTINCT postcode FROM scrapper
		        ),
			certs AS (
		        SELECT lmk_key,
		            c.postcode, c.address, u.uprn,
		            lodgement_date AS epc_date,
		            trim(current_energy_rating) AS epc_rating
		        FROM certificates_depc c
				LEFT JOIN uprn_addresses u USING (postcode, address)
		        WHERE postcode BETWEEN _r.pc_min AND _r.pc_max
				  AND TRIM(address) > ''
		          AND lodgement_date IS NOT NULL
		          AND TRIM(current_energy_rating) > ''
				),
		    matches AS (
		        SELECT *
		        FROM scrapper s
		        JOIN certs c USING (postcode, epc_date, epc_rating)
		        ),
		    /*stats AS (
		        SELECT count(distinct id) AS distinct_scrapper_id,
		            count(DISTINCT lmk_key) AS distinct_lmk_key,
		            count(NULLIF(uprn, 0)) AS cd_with_uprn,
		            count(DISTINCT NULLIF(uprn, 0)) AS distinct_cd_with_uprn,
		            count(NULLIF(trim(address), '')) AS cd_with_address,
		            count(DISTINCT NULLIF(trim(address), '')) AS distinct_cd_with_address,
		            count(DISTINCT NULLIF(trim(address), '')||epc_date::text) AS distinct_cd_with_epc_date_and_rating,
		            count(*) AS total_rows_joined
		        FROM matches
		        ), --*/
		    epc_date_and_rating_matches AS (
		        SELECT id, postcode, epc_date, epc_rating, address, uprn
		        FROM matches
				WHERE id NOT IN (SELECT id FROM matches GROUP BY id HAVING count(*) > 1)
		        )
		--SELECT * FROM stats
		--SELECT COUNT(*) FROM epc_date_and_rating_matches
		/*SELECT u.id, u.address, u.urpn,
			COALESCE(NULLIF(TRIM(u.address), ''), m.address) AS new_address,
			COALESCE(NULLIF(TRIM(u.urpn), ''), m.uprn::text) AS new_urpn
		FROM listing_scrapper_data AS u, epc_date_and_rating_matches m / */
		UPDATE listing_scrapper_data AS u SET
			address = COALESCE(NULLIF(TRIM(u.address), ''), m.address),
			urpn = COALESCE(NULLIF(TRIM(u.urpn), ''), m.uprn::text)
		FROM epc_date_and_rating_matches m -- */
		WHERE u.id = m.id
		  AND (u.address IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.address), ''), m.address)
		       OR u.urpn IS DISTINCT FROM COALESCE(NULLIF(TRIM(u.urpn), ''), m.uprn::text))
		--RETURNING u.id, u.address, u.urpn
		;
		GET DIAGNOSTICS _count = ROW_COUNT;
		RAISE NOTICE '  >> updated % listing_scrapper_data rows.', _count;
		_total := _total + COALESCE(_count, 0);

		COMMIT;
	END LOOP;
	DROP TABLE pg_temp.my_postcodes;
	RAISE NOTICE 'Finished, having updated a total of % listing_scrapper_data rows', _total;
END;
$$;


CREATE PROCEDURE public.update_minsizeft_from_text()
    LANGUAGE plpgsql
    AS $_$
BEGIN
    UPDATE listing_scrapper_data
    SET minsizeft = REPLACE(match[1], ',', '')::INT  -- Remove commas before converting to INT
    FROM (
        SELECT 
            toolkitid,
            regexp_matches(text_description, '\y([\d,]{3,})\s+sq\s*ft', 'g') AS match,
            regexp_count(text_description, '\y\d{2,4}\s+sq\s*ft') AS match_count
        FROM listing_scrapper_data
        WHERE 
            propertytype = 'flat'
            AND text_description ~ '\y\d{2,4}\s+sq\s*ft'
            AND minsizeft IS NULL
    ) AS extracted
    WHERE 
        listing_scrapper_data.toolkitid = extracted.toolkitid
        AND extracted.match_count = 1
        AND extracted.match[1] ~ '^\d{3,}$'
        AND REPLACE(extracted.match[1], ',', '')::INT >= 200;
END $_$;


CREATE PROCEDURE public.update_mobile_property_types()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET
        propertysubtype = CASE
            WHEN lsd.propertysubtype IS NULL AND lsd.propertytype IS NOT NULL THEN lsd.propertytype
            ELSE lsd.propertysubtype
        END,
        propertytype = 'mobile'
    FROM (
        SELECT id, propertysubtype, propertytype
        FROM listing_scrapper_data
        WHERE (propertysubtype ILIKE '%lodge%'
           OR propertysubtype ILIKE '%mobile%'
           OR propertysubtype ILIKE '%boat%'
           OR propertytype ILIKE '%lodge%'
           OR propertytype ILIKE '%mobile%'
           OR propertytype ILIKE '%boat%')
        AND propertysubtype NOT ILIKE '%parking%'
        AND propertysubtype NOT ILIKE '%garage%'
        AND propertytype NOT ILIKE '%parking%'
        AND propertytype NOT ILIKE '%garage%'
    ) AS lsd
    WHERE listing_scrapper_data.id = lsd.id;
END;
$$;


CREATE PROCEDURE public.update_semi_detached_to_house()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET propertysubtype = 'semi-detached',
        propertytype = 'house'
    WHERE propertysubtype = 'semi_detached';
END;
$$;


CREATE PROCEDURE public.update_shared_ownership_flag()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE listing_scrapper_data
    SET sharedownership = TRUE
    WHERE text_description ILIKE '%shared ownership%'
       OR text_description ILIKE '%share of ownership%'
       OR text_description ILIKE '%shared equity%'
       OR text_description ILIKE '%section 157%'
       OR text_description ILIKE '%s157%'
       OR text_description ILIKE '%equity share%'
       OR text_description ILIKE '%sharedownership%'
       OR text_description ~* '\m\d+%\s*share\M';
END;
$$;


CREATE PROCEDURE public.update_uprn_addresses(IN p_truncate_table boolean DEFAULT false)
    LANGUAGE plpgsql
    AS $$
DECLARE
	_batch_size int = 50000;
	_r record;
BEGIN
	IF to_regclass('public.uprn_addresses') IS NULL THEN
		RAISE NOTICE 'Creating table uprn_addresses...';
		CREATE TABLE uprn_addresses (
	        uprn bigint NOT NULL PRIMARY KEY,
	        postcode text NOT NULL COLLATE ignore_punct_case,
	        address text NOT NULL COLLATE num_ignore_punct_case
		);
	ELSIF p_truncate_table THEN
		RAISE NOTICE 'Truncating table uprn_addresses...';
		TRUNCATE TABLE public.uprn_addresses;
	END IF;

	RAISE NOTICE 'Populating uprn_addresses...';

	RAISE NOTICE 'Preparing certificates_depc in batches of % records', _batch_size;
	IF to_regclass('pg_temp.uprn_batches') IS NOT NULL THEN
		DROP TABLE pg_temp.uprn_batches;
	END IF;
	CREATE TEMP TABLE uprn_batches (
		batch int NOT NULL PRIMARY KEY,
		from_key text NOT NULL,
		to_key text NOT NULL
	);
	INSERT INTO pg_temp.uprn_batches
	SELECT batch+1, MIN(lmk_key) AS from_key, MAX(lmk_key) AS to_key
	FROM (
		SELECT ((row_number() OVER(ORDER BY lmk_key))-1)/_batch_size AS batch, lmk_key
		FROM certificates_depc
		) x
	GROUP BY 1;
			
	RAISE NOTICE 'Updating uprn_addresses from certificates_depc''s data in % batches of % rows',
		(SELECT MAX(batch) FROM pg_temp.uprn_batches), _batch_size;
	FOR _r IN SELECT * FROM pg_temp.uprn_batches ORDER BY 1
	LOOP
		RAISE NOTICE '   ...adding batch %', _r.batch;
		INSERT INTO uprn_addresses
		SELECT u.uprn::bigint, u.postcode, u.address
		FROM certificates_depc u
		WHERE u.lmk_key BETWEEN _r.from_key AND _r.to_key
		  AND u.uprn <> 'NaN' AND u.postcode > '' AND u.address > ''
		  AND NOT EXISTS (SELECT * FROM uprn_addresses ua WHERE ua.uprn = u.uprn::bigint)
		ON CONFLICT (uprn) DO NOTHING;

		COMMIT;
	END LOOP;

	DROP TABLE pg_temp.uprn_batches;

	IF to_regclass('uprn_addresses_postcode_address_idx') IS NULL THEN
		RAISE NOTICE 'Indexing uprn_addresses...';
		CREATE INDEX uprn_addresses_postcode_address_idx ON uprn_addresses (postcode, address);
	END IF;

	RAISE NOTICE 'Done.';
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE public.old_price_paid (
    ppid text,
    price_paid text,
    sold_date text,
    postcode text,
    poan text,
    soan text,
    street text,
    locality text,
    town text,
    district text,
    county text,
    toolkitid text,
    uprn numeric,
    latitude text,
    longitude text
);


CREATE TABLE eth.dup_ppid (
    ppid text NOT NULL,
    pp_rows public.old_price_paid[]
);


CREATE TABLE eth.pp_addresses (
    address_id integer NOT NULL,
    poan text COLLATE public.ignore_punct_case,
    soan text COLLATE public.ignore_punct_case,
    uprn bigint,
    postcode_id integer,
    street_id integer,
    locality_id smallint,
    town_id smallint,
    district_id smallint,
    county_id smallint,
    latitude double precision,
    longitude double precision
);


ALTER TABLE eth.pp_addresses ALTER COLUMN address_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME eth.pp_addresses_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_counties (
    county_id smallint NOT NULL,
    county text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_counties ALTER COLUMN county_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_counties_county_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_districts (
    district_id smallint NOT NULL,
    district text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_districts ALTER COLUMN district_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_districts_district_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_localities (
    locality_id smallint NOT NULL,
    locality text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_localities ALTER COLUMN locality_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_localities_locality_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_postcodes (
    postcode_id integer NOT NULL,
    postcode text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_postcodes ALTER COLUMN postcode_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_postcodes_postcode_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_streets (
    street_id integer NOT NULL,
    street text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_streets ALTER COLUMN street_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_streets_street_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.pp_towns (
    town_id smallint NOT NULL,
    town text NOT NULL COLLATE public.ignore_punct_case
);


ALTER TABLE eth.pp_towns ALTER COLUMN town_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME eth.pp_towns_town_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE eth.price_paid_data (
    address_id integer NOT NULL,
    sold_date date NOT NULL,
    price_paid integer NOT NULL,
    ppid uuid NOT NULL
);


CREATE VIEW eth.v_pp_addresses AS
 SELECT a.address_id,
    a.poan,
    a.soan,
    pc.postcode,
    st.street,
    l.locality,
    t.town,
    d.district,
    c.county,
    a.uprn,
    a.latitude,
    a.longitude
   FROM ((((((eth.pp_addresses a
     LEFT JOIN eth.pp_postcodes pc USING (postcode_id))
     LEFT JOIN eth.pp_streets st USING (street_id))
     LEFT JOIN eth.pp_localities l USING (locality_id))
     LEFT JOIN eth.pp_towns t USING (town_id))
     LEFT JOIN eth.pp_districts d USING (district_id))
     LEFT JOIN eth.pp_counties c USING (county_id));


CREATE VIEW eth.price_paid AS
 SELECT pp.ppid,
    pp.price_paid,
    pp.sold_date,
    a.postcode,
    a.poan,
    a.soan,
    a.street,
    a.locality,
    a.town,
    a.district,
    a.county,
    NULL::text AS toolkitid,
    a.uprn,
    a.latitude,
    a.longitude
   FROM (eth.price_paid_data pp
     JOIN eth.v_pp_addresses a USING (address_id));


CREATE TABLE public.air_availability (
    listing_id bigint NOT NULL,
    date date NOT NULL,
    bookable boolean,
    available boolean
);


CREATE TABLE public.air_listing_map (
    listing_id bigint NOT NULL,
    country character varying(2),
    city character varying(100),
    state character varying(100),
    type character varying(100),
    CONSTRAINT chk_country CHECK (((country IS NULL) OR (length((country)::text) = 2)))
);


CREATE TABLE public.air_listings (
    listing_id bigint NOT NULL,
    location_latitude numeric(10,8),
    location_longitude numeric(10,8),
    description text,
    title character varying(1000),
    images text[],
    amenities json,
    bedrooms integer,
    bathrooms integer,
    property_type character varying(100),
    host_name character varying(50),
    host_link character varying(100),
    address character varying(1000),
    superhost_status boolean,
    accuracy_rating numeric(3,2),
    checkin_rating numeric(3,2),
    cleanliness_rating numeric(3,2),
    communication_rating numeric(3,2),
    location_rating numeric(3,2),
    value_rating numeric(3,2),
    guest_satisfaction_overall numeric(3,2),
    visible_review integer,
    urpn numeric
);


CREATE TABLE public.air_price (
    id integer NOT NULL,
    listing_id bigint,
    date timestamp with time zone,
    night_rate_1 numeric(10,2),
    night_rate_3 numeric(10,2),
    night_rate_7 numeric(10,2),
    month_rate numeric(10,2),
    random_rate numeric(10,2)
);


ALTER TABLE public.air_price ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.air_price_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE public.air_status (
    id integer NOT NULL,
    listing_id bigint,
    date timestamp with time zone,
    status character varying(10)
);


ALTER TABLE public.air_status ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.air_status_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE public.brand_aliases (
    id integer NOT NULL,
    original_name text NOT NULL,
    cleaned_name text NOT NULL,
    canonical_name text
);


CREATE TABLE public.brand_aliases_backup (
    toolkitid character varying(255),
    original_name character varying(255)
);


CREATE SEQUENCE public.brand_aliases_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.brand_aliases_id_seq OWNED BY public.brand_aliases.id;


CREATE TABLE public.certificates_depc (
    lmk_key text NOT NULL,
    address1 text,
    address2 text,
    address3 text,
    postcode text,
    building_reference_number text,
    current_energy_rating text,
    potential_energy_rating text,
    current_energy_efficiency numeric,
    potential_energy_efficiency numeric,
    property_type text,
    built_form text,
    inspection_date date,
    local_authority text,
    constituency text,
    county text,
    lodgement_date date,
    transaction_type text,
    environmental_impact_current numeric,
    environmental_impact_potential numeric,
    energy_consumption_current real,
    energy_consumption_potential real,
    co2_emissions_current numeric,
    co2_emiss_curr_per_floor_area numeric,
    co2_emissions_potential numeric,
    lighting_cost_current real,
    lighting_cost_potential real,
    heating_cost_current real,
    heating_cost_potential real,
    hot_water_cost_current real,
    hot_water_cost_potential real,
    total_floor_area numeric,
    energy_tariff text,
    mains_gas_flag text,
    floor_level text,
    flat_top_storey text,
    flat_storey_count numeric,
    main_heating_controls text,
    multi_glaze_proportion real,
    glazed_type text,
    glazed_area text,
    extension_count numeric,
    number_habitable_rooms real,
    number_heated_rooms real,
    low_energy_lighting numeric,
    number_open_fireplaces numeric,
    hotwater_description text,
    hot_water_energy_eff text,
    hot_water_env_eff text,
    floor_description text,
    floor_energy_eff text,
    floor_env_eff text,
    windows_description text,
    windows_energy_eff text,
    windows_env_eff text,
    walls_description text,
    walls_energy_eff text,
    walls_env_eff text,
    secondheat_description text,
    sheating_energy_eff text,
    sheating_env_eff text,
    roof_description text,
    roof_energy_eff text,
    roof_env_eff text,
    mainheat_description text,
    mainheat_energy_eff text,
    mainheat_env_eff text,
    mainheatcont_description text,
    mainheatc_energy_eff text,
    mainheatc_env_eff text,
    lighting_description text,
    lighting_energy_eff text,
    lighting_env_eff text,
    main_fuel text,
    wind_turbine_count real,
    heat_loss_corridor text,
    unheated_corridor_length numeric,
    floor_height numeric,
    photo_supply real,
    solar_water_heating_flag text,
    mechanical_ventilation text,
    address text,
    local_authority_label text,
    constituency_label text,
    posttown text,
    construction_age_band text,
    lodgement_datetime timestamp without time zone,
    tenure text,
    fixed_lighting_outlets_count real,
    low_energy_fixed_light_count real,
    uprn numeric,
    uprn_source text,
    toolkitid text,
    latitude text,
    longitude text
);


CREATE TABLE public.certificates_non_depc (
    lmk_key text NOT NULL,
    address1 text,
    address2 text,
    address3 text,
    postcode text,
    building_reference_number text,
    asset_rating numeric,
    asset_rating_band text,
    property_type text,
    inspection_date date,
    local_authority text,
    constituency text,
    county text,
    lodgement_date date,
    transaction_type text,
    new_build_benchmark text,
    existing_stock_benchmark text,
    building_level text,
    main_heating_fuel text,
    other_fuel_desc text,
    special_energy_uses text,
    renewable_sources text,
    floor_area numeric,
    standard_emissions numeric,
    target_emissions numeric,
    typical_emissions numeric,
    building_emissions numeric,
    aircon_present text,
    aircon_kw_rating numeric,
    estimated_aircon_kw_rating numeric,
    ac_inspection_commissioned numeric,
    building_environment text,
    address text,
    local_authority_label text,
    constituency_label text,
    posttown text,
    lodgement_datetime text,
    primary_energy_value numeric,
    uprn numeric,
    uprn_source text,
    toolkitid text,
    latitude text,
    longitude text
);


CREATE TABLE public.cities (
    id integer NOT NULL,
    "Location" character varying(50),
    "Extension" character varying(50),
    portal character varying(50)
);


CREATE TABLE public.hmo_index (
    id integer NOT NULL,
    property_id character varying(50),
    toolkitid character varying(100),
    date timestamp with time zone,
    occupancy integer,
    occupancy_total integer,
    price numeric(10,2),
    status character varying(10),
    updates character varying(100),
    source character varying(30),
    not_found integer
);


ALTER TABLE public.hmo_index ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.hmo_index_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


CREATE TABLE public.hmo_listings_afs (
    property_id character varying(50) NOT NULL,
    uprn numeric,
    toolkitid character varying(100),
    property_type character varying(100),
    location_latitude numeric(10,8),
    location_longitude numeric(10,8),
    images text[],
    address1 character varying(100),
    address2 character varying(100),
    city character varying(100),
    postcode character varying(20),
    bills_included character varying(100),
    description text,
    amenities json,
    available date,
    landlord_company_name character varying(100),
    landlord_contact_numbers character varying(100),
    landlord_first_name character varying(100),
    landlord_last_name character varying(100),
    landlord_email character varying(100),
    landlord_provider_type character varying(100),
    bathrooms integer,
    bedrooms integer,
    bills character varying(20),
    url text,
    rent_ppw numeric(10,2),
    rent_pcm numeric(10,2),
    rooms json
);


CREATE TABLE public.hmo_listings_spr (
    property_id character varying(50) NOT NULL,
    uprn numeric,
    toolkitid character varying(100),
    property_type character varying(100),
    tenant_type character varying(20),
    rent_options character varying(100),
    location_latitude numeric(10,8),
    location_longitude numeric(10,8),
    images text[],
    postcode character varying(20),
    bills_included character varying(100),
    description text,
    living_room character varying(20),
    furnishings character varying(20),
    broadband character varying(20),
    balcony character varying(20),
    parking character varying(20),
    garden character varying(20),
    bedrooms integer,
    landlord_provider_type character varying(100),
    landlord_name character varying(100),
    min_term integer,
    max_term integer,
    available date,
    url text,
    rent_ppw numeric(10,2),
    rent_pcm numeric(10,2),
    rooms json
);


CREATE TABLE public.hmo_listings_uni (
    property_id character varying(50) NOT NULL,
    uprn numeric,
    toolkitid character varying(100),
    property_type character varying(100),
    sub_type character varying(100),
    location_latitude numeric(10,8),
    location_longitude numeric(10,8),
    images text[],
    floorplans text[],
    house_no character varying(100),
    street character varying(100),
    postcode character varying(20),
    landlord_first_name character varying(100),
    landlord_last_name character varying(100),
    landlord_company character varying(100),
    landlord_name character varying(100),
    epc character varying(10),
    description text,
    features text[],
    available date,
    made_live_at date,
    updated_at date,
    bathrooms integer,
    bedrooms integer,
    rent_ppw numeric(10,2),
    rent_pcm numeric(10,2),
    url character varying(200),
    rooms json
);


CREATE TABLE public.listing_scrapper_data (
    brandname character varying(255),
    minsizeft numeric,
    ownership character varying(255),
    postcode character varying(255),
    preowned boolean,
    propertysubtype character varying(255),
    propertytype character varying(255),
    retirement boolean,
    soldstc boolean,
    brochures_url json,
    epcgraphs_url json,
    floorplans_caption json,
    floorplans_url json,
    portalid integer,
    images_url json,
    annualgroundrent numeric,
    annualservicecharge numeric,
    location_latitude numeric,
    location_longitude numeric,
    sharedownership boolean,
    status_archived boolean,
    status_published boolean,
    tenuretype character varying(255),
    yearsremainingonlease integer,
    text_description text,
    toolkitid character varying(255),
    id integer NOT NULL,
    uprn bigint,
    address text DEFAULT ''::text,
    epc_current_value integer DEFAULT 0,
    epc_potential_value integer DEFAULT 0,
    epc_values_added boolean DEFAULT false,
    transactionhistoryprice numeric,
    transactionhistoryyear integer,
    transactionhistory_done boolean,
    epc_date character varying(255) DEFAULT ''::character varying,
    epc_rating character varying(255) DEFAULT ''::character varying,
    display_address character varying(255),
    agent_logo character varying(255)
);


CREATE SEQUENCE public.listing_scrapper_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.listing_scrapper_data_id_seq OWNED BY public.listing_scrapper_data.id;


CREATE TABLE public.ni_listings (
    id integer NOT NULL,
    portalid bigint NOT NULL,
    sourcefrom character varying(50),
    brandname character varying(255),
    agent_logo text,
    postcode character varying(20),
    propertysub_type character varying(100),
    propertytype character varying(100),
    brochures_url text,
    location_latitude numeric,
    location_longitude numeric,
    address text,
    images_url jsonb,
    floorplans_url jsonb,
    description text,
    epc_current integer,
    epc_potential integer,
    epc_rating character(1),
    tenuretype character varying(100),
    soldstc boolean,
    preowned boolean,
    created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.ni_listings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ni_listings_id_seq OWNED BY public.ni_listings.id;


CREATE TABLE public.ni_results (
    id integer NOT NULL,
    sourcefrom character varying(50) NOT NULL,
    portalid bigint NOT NULL,
    bedrooms integer,
    bathrooms integer,
    floorplans text,
    price integer,
    channel character varying(50),
    displaystatus character varying(50),
    listingupdate character varying(50),
    listingdate timestamp without time zone,
    region character varying(255),
    last_found timestamp without time zone,
    listing_crawled boolean DEFAULT false,
    created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.ni_results_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ni_results_id_seq OWNED BY public.ni_results.id;


CREATE TABLE public.ni_updates (
    id integer NOT NULL,
    portalid bigint,
    change_type character varying(100),
    old_value text,
    new_value text,
    date_detected timestamp without time zone,
    date_recorded timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.ni_updates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ni_updates_id_seq OWNED BY public.ni_updates.id;


CREATE TABLE public.osopenuprn (
    uprn bigint NOT NULL,
    x_coordinate double precision NOT NULL,
    y_coordinate double precision NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL
);


CREATE TABLE public.outcodes (
    id integer NOT NULL,
    postcode character varying(10),
    latitude double precision,
    longitude double precision
);


CREATE SEQUENCE public.outcodes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.outcodes_id_seq OWNED BY public.outcodes.id;


CREATE TABLE public.price_paid (
    ppid uuid NOT NULL,
    price_paid bigint,
    sold_date timestamp without time zone,
    postcode character varying(10),
    poan character varying(100),
    soan character varying(100),
    street character varying(100),
    locality character varying(100),
    town character varying(100),
    district character varying(100),
    county character varying(100),
    toolkitid character varying(100),
    uprn bigint,
    latitude numeric(13,10),
    longitude numeric(13,10)
);


CREATE TABLE public.price_paid_uprn_matches (
    ppid uuid NOT NULL,
    uprn bigint,
    matches smallint,
    errors smallint
);


CREATE TABLE public.propertytool (
    id bigint NOT NULL,
    bedrooms integer,
    bathrooms integer,
    floorplans integer,
    price numeric,
    channel text,
    displaystatus text,
    listingupdate text,
    listingdate timestamp without time zone,
    region text,
    toolkitid character varying NOT NULL,
    not_found integer DEFAULT 0,
    listing_done integer DEFAULT 0,
    sourcefrom character varying(255) DEFAULT 'Rightmove'::character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.propertytool_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.propertytool_id_seq OWNED BY public.propertytool.id;


CREATE TABLE public.proxies (
    id numeric NOT NULL,
    reference character varying,
    active character varying,
    provider character varying,
    websites character varying,
    connect_address character varying(32767),
    connect_port character varying(32767),
    proxy_address character varying(32767),
    expiry_date character varying(32767),
    username character varying(32767),
    password character varying(32767)
);


CREATE TABLE public.rightmove_region (
    id integer NOT NULL,
    outcode character varying(50),
    region character varying(75)
);


CREATE SEQUENCE public.rightmove_region_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.rightmove_region_id_seq OWNED BY public.rightmove_region.id;


CREATE TABLE public.target_proxies (
    ip inet NOT NULL,
    port integer NOT NULL,
    target text,
    status boolean DEFAULT true NOT NULL
);


CREATE TABLE public.uk_locations (
    id integer NOT NULL,
    location character varying(50),
    population integer NOT NULL,
    geotype character varying(50)
);


CREATE SEQUENCE public.uk_locations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.uk_locations_id_seq OWNED BY public.uk_locations.id;


CREATE TABLE public.updated_property_status (
    id integer NOT NULL,
    portalid integer,
    toolkitid character varying,
    price bigint,
    update character varying(255),
    date timestamp without time zone,
    status character varying(255)
);


CREATE SEQUENCE public.updated_property_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.updated_property_status_id_seq OWNED BY public.updated_property_status.id;


CREATE TABLE public.uprn_addresses (
    uprn bigint NOT NULL,
    postcode text NOT NULL COLLATE public.ignore_punct_case,
    address text NOT NULL COLLATE public.num_ignore_punct_case
);


CREATE TABLE public.uprn_records (
    uprn numeric NOT NULL,
    x_coordinate text,
    y_coordinate text,
    latitude text,
    longitude text
);


ALTER TABLE ONLY public.brand_aliases ALTER COLUMN id SET DEFAULT nextval('public.brand_aliases_id_seq'::regclass);


ALTER TABLE ONLY public.listing_scrapper_data ALTER COLUMN id SET DEFAULT nextval('public.listing_scrapper_data_id_seq'::regclass);


ALTER TABLE ONLY public.ni_listings ALTER COLUMN id SET DEFAULT nextval('public.ni_listings_id_seq'::regclass);


ALTER TABLE ONLY public.ni_results ALTER COLUMN id SET DEFAULT nextval('public.ni_results_id_seq'::regclass);


ALTER TABLE ONLY public.ni_updates ALTER COLUMN id SET DEFAULT nextval('public.ni_updates_id_seq'::regclass);


ALTER TABLE ONLY public.outcodes ALTER COLUMN id SET DEFAULT nextval('public.outcodes_id_seq'::regclass);


ALTER TABLE ONLY public.propertytool ALTER COLUMN id SET DEFAULT nextval('public.propertytool_id_seq'::regclass);


ALTER TABLE ONLY public.rightmove_region ALTER COLUMN id SET DEFAULT nextval('public.rightmove_region_id_seq'::regclass);


ALTER TABLE ONLY public.uk_locations ALTER COLUMN id SET DEFAULT nextval('public.uk_locations_id_seq'::regclass);


ALTER TABLE ONLY public.updated_property_status ALTER COLUMN id SET DEFAULT nextval('public.updated_property_status_id_seq'::regclass);


ALTER TABLE ONLY eth.dup_ppid
    ADD CONSTRAINT dup_ppid_pkey PRIMARY KEY (ppid);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_address_key UNIQUE (postcode_id, poan, soan, street_id, locality_id, town_id, district_id, county_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_pkey PRIMARY KEY (address_id);


ALTER TABLE ONLY eth.pp_counties
    ADD CONSTRAINT pp_counties_county_key UNIQUE (county);


ALTER TABLE ONLY eth.pp_counties
    ADD CONSTRAINT pp_counties_pkey PRIMARY KEY (county_id);


ALTER TABLE ONLY eth.pp_districts
    ADD CONSTRAINT pp_districts_district_key UNIQUE (district);


ALTER TABLE ONLY eth.pp_districts
    ADD CONSTRAINT pp_districts_pkey PRIMARY KEY (district_id);


ALTER TABLE ONLY eth.pp_localities
    ADD CONSTRAINT pp_localities_locality_key UNIQUE (locality);


ALTER TABLE ONLY eth.pp_localities
    ADD CONSTRAINT pp_localities_pkey PRIMARY KEY (locality_id);


ALTER TABLE ONLY eth.pp_postcodes
    ADD CONSTRAINT pp_postcodes_pkey PRIMARY KEY (postcode_id);


ALTER TABLE ONLY eth.pp_postcodes
    ADD CONSTRAINT pp_postcodes_postcode_key UNIQUE (postcode);


ALTER TABLE ONLY eth.pp_streets
    ADD CONSTRAINT pp_streets_pkey PRIMARY KEY (street_id);


ALTER TABLE ONLY eth.pp_streets
    ADD CONSTRAINT pp_streets_street_key UNIQUE (street);


ALTER TABLE ONLY eth.pp_towns
    ADD CONSTRAINT pp_towns_pkey PRIMARY KEY (town_id);


ALTER TABLE ONLY eth.pp_towns
    ADD CONSTRAINT pp_towns_town_key UNIQUE (town);


ALTER TABLE ONLY eth.price_paid_data
    ADD CONSTRAINT price_paid_data_pkey PRIMARY KEY (address_id, sold_date);


ALTER TABLE ONLY public.air_availability
    ADD CONSTRAINT air_availability_pkey PRIMARY KEY (listing_id, date);


ALTER TABLE ONLY public.air_listing_map
    ADD CONSTRAINT air_listing_map_pkey PRIMARY KEY (listing_id);


ALTER TABLE ONLY public.air_listings
    ADD CONSTRAINT air_listings_pkey PRIMARY KEY (listing_id);


ALTER TABLE ONLY public.air_price
    ADD CONSTRAINT air_price_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.air_status
    ADD CONSTRAINT air_status_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.brand_aliases
    ADD CONSTRAINT brand_aliases_original_name_key UNIQUE (original_name);


ALTER TABLE ONLY public.brand_aliases
    ADD CONSTRAINT brand_aliases_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.certificates_depc
    ADD CONSTRAINT certificates_depc_pkey PRIMARY KEY (lmk_key);


ALTER TABLE ONLY public.certificates_non_depc
    ADD CONSTRAINT certificates_non_depc_pkey PRIMARY KEY (lmk_key);


ALTER TABLE ONLY public.hmo_index
    ADD CONSTRAINT hmo_index_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.hmo_listings_afs
    ADD CONSTRAINT hmo_listings_afs_pkey PRIMARY KEY (property_id);


ALTER TABLE ONLY public.hmo_listings_spr
    ADD CONSTRAINT hmo_listings_spr_pkey PRIMARY KEY (property_id);


ALTER TABLE ONLY public.hmo_listings_uni
    ADD CONSTRAINT hmo_listings_uni_pkey PRIMARY KEY (property_id);


ALTER TABLE ONLY public.listing_scrapper_data
    ADD CONSTRAINT listing_scrapper_data_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.price_paid
    ADD CONSTRAINT new_price_paid_pkey PRIMARY KEY (ppid);


ALTER TABLE ONLY public.ni_listings
    ADD CONSTRAINT ni_listings_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.ni_listings
    ADD CONSTRAINT ni_listings_portalid_key UNIQUE (portalid);


ALTER TABLE ONLY public.ni_results
    ADD CONSTRAINT ni_results_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.ni_results
    ADD CONSTRAINT ni_results_portalid_sourcefrom_key UNIQUE (portalid, sourcefrom);


ALTER TABLE ONLY public.ni_updates
    ADD CONSTRAINT ni_updates_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.osopenuprn
    ADD CONSTRAINT osopenuprn_pkey PRIMARY KEY (uprn);


ALTER TABLE ONLY public.outcodes
    ADD CONSTRAINT outcodes_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.price_paid_uprn_matches
    ADD CONSTRAINT price_paid_uprn_matches_pkey PRIMARY KEY (ppid);


ALTER TABLE ONLY public.propertytool
    ADD CONSTRAINT propertytool_pkey PRIMARY KEY (toolkitid);


ALTER TABLE ONLY public.proxies
    ADD CONSTRAINT proxies_unique UNIQUE (id);


ALTER TABLE ONLY public.rightmove_region
    ADD CONSTRAINT rightmove_region_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.uk_locations
    ADD CONSTRAINT uk_locations_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.listing_scrapper_data
    ADD CONSTRAINT unique_portalid UNIQUE (portalid);


ALTER TABLE ONLY public.updated_property_status
    ADD CONSTRAINT updated_property_status_pkey PRIMARY KEY (id);


ALTER TABLE ONLY public.uprn_addresses
    ADD CONSTRAINT uprn_addresses_pkey PRIMARY KEY (uprn);


ALTER TABLE ONLY public.uprn_records
    ADD CONSTRAINT uprn_records_pkey PRIMARY KEY (uprn);


CREATE INDEX pp_addresses_county_id_idx ON eth.pp_addresses USING btree (county_id);


CREATE INDEX pp_addresses_district_id_idx ON eth.pp_addresses USING btree (district_id);


CREATE INDEX pp_addresses_locality_id_idx ON eth.pp_addresses USING btree (locality_id);


CREATE INDEX pp_addresses_street_id_idx ON eth.pp_addresses USING btree (street_id);


CREATE INDEX pp_addresses_town_id_idx ON eth.pp_addresses USING btree (town_id);


CREATE INDEX price_paid_data_address_id_idx ON eth.price_paid_data USING btree (ppid);


CREATE INDEX price_paid_data_price_paid_idx ON eth.price_paid_data USING btree (price_paid);


CREATE INDEX price_paid_data_sold_date_idx ON eth.price_paid_data USING btree (sold_date);


CREATE INDEX certificates_depc_uprn_idx ON public.certificates_depc USING btree (uprn);


CREATE INDEX certificates_non_depc_uprn_idx ON public.certificates_non_depc USING btree (uprn);


CREATE UNIQUE INDEX cities_id_idx ON public.cities USING btree (id);


CREATE INDEX depc_postcode_idx ON public.certificates_depc USING btree (postcode);


CREATE INDEX idx_air_available ON public.air_availability USING btree (available);


CREATE INDEX idx_air_bookable ON public.air_availability USING btree (bookable);


CREATE INDEX idx_air_date ON public.air_status USING btree (date);


CREATE INDEX idx_air_month ON public.air_price USING btree (month_rate);


CREATE INDEX idx_air_price_date ON public.air_price USING btree (date);


CREATE INDEX idx_air_price_listing_id ON public.air_price USING btree (listing_id);


CREATE INDEX idx_air_rate_1 ON public.air_price USING btree (night_rate_1);


CREATE INDEX idx_air_rate_3 ON public.air_price USING btree (night_rate_3);


CREATE INDEX idx_air_rate_7 ON public.air_price USING btree (night_rate_7);


CREATE INDEX idx_air_status ON public.air_status USING btree (status);


CREATE INDEX idx_certificates_depc_nan_filter ON public.certificates_depc USING btree (lmk_key) WHERE ((address1 = 'NaN'::text) OR (address2 = 'NaN'::text) OR (address3 = 'NaN'::text) OR (county = 'NaN'::text) OR (flat_storey_count = 'NaN'::numeric) OR (floor_energy_eff = 'NaN'::text) OR (floor_env_eff = 'NaN'::text) OR (secondheat_description = 'NaN'::text) OR (sheating_energy_eff = 'NaN'::text) OR (sheating_env_eff = 'NaN'::text) OR (unheated_corridor_length = 'NaN'::numeric) OR (floor_height = 'NaN'::numeric) OR (photo_supply = 'NaN'::real) OR (solar_water_heating_flag = 'NaN'::text) OR (uprn = 'NaN'::numeric) OR (uprn_source = 'NaN'::text));


CREATE INDEX idx_certificates_lmk ON public.certificates_depc USING btree (lmk_key);


CREATE INDEX idx_date_hmo_index ON public.hmo_index USING btree (date);


CREATE INDEX idx_listing_map_country ON public.air_listing_map USING btree (country);


CREATE INDEX idx_listing_scrapper_data_portalid ON public.listing_scrapper_data USING btree (portalid);


CREATE INDEX idx_listing_scrapper_data_urpn_epc ON public.listing_scrapper_data USING btree (uprn, epc_current_value);


CREATE INDEX idx_listing_status ON public.air_status USING btree (listing_id);


CREATE INDEX idx_not_found_hmo_index ON public.hmo_index USING btree (not_found);


CREATE INDEX idx_price_paid_amount ON public.price_paid USING btree (price_paid);


CREATE INDEX idx_price_paid_postcode ON public.price_paid USING btree (postcode);


CREATE INDEX idx_price_paid_postcode_price ON public.price_paid USING btree (postcode, price_paid);


CREATE INDEX idx_price_paid_sold_date_ppid ON public.price_paid USING btree (sold_date, ppid);


CREATE INDEX idx_price_paid_uprn ON public.price_paid USING btree (uprn);


CREATE INDEX idx_property_id_hmo_index ON public.hmo_index USING btree (property_id);


CREATE INDEX idx_propertytool_id_sourcefrom ON public.propertytool USING btree (id, sourcefrom);


CREATE INDEX idx_random_rate ON public.air_price USING btree (random_rate);


CREATE INDEX idx_status_hmo_index ON public.hmo_index USING btree (status);


CREATE INDEX idx_uprn_addresses_postcode ON public.uprn_addresses USING btree (postcode);


CREATE INDEX listing_scrapper_data_postcode_idx ON public.listing_scrapper_data USING btree (postcode);


CREATE INDEX listing_scrapper_data_toolkitid_idx ON public.listing_scrapper_data USING btree (toolkitid);


CREATE INDEX listing_scrapper_data_urpn_idx ON public.listing_scrapper_data USING btree (uprn);


CREATE INDEX price_paid_postcode_idx ON public.old_price_paid USING btree (postcode);


CREATE INDEX price_paid_postcode_no_uprn_idx ON public.old_price_paid USING btree (split_part(upper(postcode), ' '::text, 1)) WHERE (uprn IS NULL);


CREATE INDEX price_paid_postcode_split_part_idx ON public.old_price_paid USING btree (split_part(upper(postcode), ' '::text, 1));


CREATE INDEX price_paid_uprn_index ON public.old_price_paid USING btree (uprn);


CREATE INDEX price_paid_uprn_matches_uprn_idx ON public.price_paid_uprn_matches USING btree (uprn);


CREATE INDEX propertytool_id_idx ON public.propertytool USING btree (id);


CREATE INDEX updated_property_status_portalid_idx ON public.updated_property_status USING btree (portalid);


CREATE INDEX updated_property_status_toolkitid_idx ON public.updated_property_status USING btree (toolkitid);


CREATE INDEX uprn_addresses_postcode_address_idx ON public.uprn_addresses USING btree (postcode, address);


CREATE TRIGGER price_paid_dummy_insert_tg INSTEAD OF INSERT ON eth.price_paid FOR EACH ROW EXECUTE FUNCTION eth.price_paid_insert_tg();


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_county_id_fkey FOREIGN KEY (county_id) REFERENCES eth.pp_counties(county_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_district_id_fkey FOREIGN KEY (district_id) REFERENCES eth.pp_districts(district_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_locality_id_fkey FOREIGN KEY (locality_id) REFERENCES eth.pp_localities(locality_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_postcode_id_fkey FOREIGN KEY (postcode_id) REFERENCES eth.pp_postcodes(postcode_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_street_id_fkey FOREIGN KEY (street_id) REFERENCES eth.pp_streets(street_id);


ALTER TABLE ONLY eth.pp_addresses
    ADD CONSTRAINT pp_addresses_town_id_fkey FOREIGN KEY (town_id) REFERENCES eth.pp_towns(town_id);


ALTER TABLE ONLY eth.price_paid_data
    ADD CONSTRAINT price_paid_data_address_id_fkey FOREIGN KEY (address_id) REFERENCES eth.pp_addresses(address_id);



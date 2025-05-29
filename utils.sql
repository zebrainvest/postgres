CREATE OR REPLACE FUNCTION public.get_row_count(regclass) RETURNS bigint AS
$BODY$
DECLARE _cnt bigint;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %s', $1) INTO _cnt;
    RETURN _cnt;
END
$BODY$ LANGUAGE plpgsql;

/*
SELECT string_agg(
    format('* %s (%s)', table_name, to_char(get_row_count(format('%s.%s', table_schema, table_name)::regclass), 'FM9,999,999')),
    E'\n' ORDER BY table_name)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
*/
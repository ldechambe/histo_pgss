-- Extension to historize pg_stat_statements ------------------------------------
--
-- The necessary tables are installed in public 

-- INSTALLATION ----------------------------------------------------------------------------

\echo Use "CREATE EXTENSION histo_pgss" to load this file. \quit

-- Table listing snapshots
-- DROP TABLE IF EXISTS public.histo_pgss_snapshots;
CREATE TABLE public.histo_pgss_snapshots (
 snapshot_id    serial
,snapshot_ts    timestamptz
,snapshot_comment varchar
);

-- Table containing pg_stat_statements history
--DROP TABLE IF EXISTS public.histo_pgss_snapshot_details;
CREATE TABLE public.histo_pgss_snapshot_details AS
SELECT 1::integer AS snapshot_id
      ,NULL::text AS query_md5
      ,s.*
FROM pg_stat_statements s
LIMIT 0;

-- "Take a snapshot" function
-- DROP FUNCTION IF EXISTS public.histo_pgss_snapshot(varchar);
CREATE OR REPLACE FUNCTION public.histo_pgss_snapshot (
    p_comment VARCHAR default current_timestamp::varchar
)
RETURNS integer AS
$$
DECLARE
    l_snapshot_id    integer;
BEGIN

    -- Create snapshot entry
    INSERT INTO public.histo_pgss_snapshots (snapshot_ts,snapshot_comment)
    VALUES (current_timestamp,p_comment)
    RETURNING snapshot_id INTO l_snapshot_id;

    -- Copy content of pg_stat_statements into history table
    INSERT INTO  public.histo_pgss_snapshot_details
    SELECT l_snapshot_id as snapshot_id
          ,md5(s.query)
          ,s.*
    FROM pg_stat_statements s;

    -- Reset pg_stat_statements
    PERFORM pg_stat_statements_reset();

    RETURN l_snapshot_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION histo_pgss_snapshot(varchar) IS '
Adds a snapshot, copy content of pg_stat_statements into history table and resets pg_stat_statements.
';

-- "Purge" function
-- DROP FUNCTION IF EXISTS public.histo_pgss_purge(timestamp);
CREATE OR REPLACE FUNCTION public.histo_pgss_purge (
    p_timestamp timestamptz default current_timestamp - INTERVAL '30 days'
)
RETURNS varchar AS
$$
DECLARE
    l_nb_lines    integer;
    l_return       varchar;
BEGIN

    -- Remove snapshots older than parameter
    DELETE FROM public.histo_pgss_snapshots
    WHERE snapshot_ts < p_timestamp;

    GET DIAGNOSTICS l_nb_lines = ROW_COUNT;

    l_return = CONCAT(l_nb_lines, ' snapshot(s) removed; ');

    -- Remove historized queries with no snapshot references
    DELETE FROM public.histo_pgss_snapshot_details
    WHERE snapshot_id NOT IN (SELECT snapshot_id FROM public.histo_pgss_snapshots);

    GET DIAGNOSTICS l_nb_lines = ROW_COUNT;

    l_return = CONCAT(l_return, l_nb_lines, ' detail(s) removed;');
    RETURN l_return;

END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION histo_pgss_purge(timestamptz) IS '
Cleansing of histo_pgss tables.
';

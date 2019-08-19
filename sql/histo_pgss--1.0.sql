-- Extension to historize pg_stat_statements ------------------------------------
--
-- The necessary tables are installed in public 

-- INSTALLATION ----------------------------------------------------------------------------

\echo Use "CREATE EXTENSION histo_pgss" to load this file. \quit

-- Table listing snapshots
-- DROP TABLE IF EXISTS public.histo_pgss_snapshots;
CREATE TABLE public.histo_pgss_snapshots (
 snapshot_id    serial  PRIMARY KEY
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

ALTER TABLE histo_pgss_snapshot_details
ADD CONSTRAINT fk_histo_pgss_snapshots_id FOREIGN KEY (snapshot_id) REFERENCES histo_pgss_snapshots (snapshot_id);



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


-- "Aggregation" function
-- DROP FUNCTION IF EXISTS public.histo_pgss_aggregate(timestamptz, varchar);
CREATE OR REPLACE FUNCTION public.histo_pgss_aggregate (
    p_max_timestamp timestamptz default current_timestamp - INTERVAL '30 days'
   ,p_aggr_level    varchar     default 'DAY'
)
RETURNS varchar AS
$$
DECLARE
    l_return        VARCHAR;
    l_aggr          RECORD;
    l_snapshot_id   INTEGER;
    
    l_snap_aggr_count   INTEGER     :=0;
    l_snap_count        INTEGER     :=0;
    l_nb_rows           INTEGER;
    
BEGIN
    
    -- Check aggregation level
    IF UPPER(p_aggr_level) NOT IN ('YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE')
    THEN
        RETURN 'WARNING : Nothing done; p_aggr_level can only be ''YEAR'', ''MONTH'', ''WEEK'', ''DAY'', ''HOUR'' or ''MINUTE''';
    END IF;
    
    -- Loop on aggregate timestamps to create
    FOR l_aggr IN(
        SELECT (date_trunc(p_aggr_level, snapshot_ts) + INTERVAL '0 second' + ('1 '||p_aggr_level)::interval) AS aggr_snapshot_ts   
             , array_agg(snapshot_id) as snapshot_array
          FROM histo_pgss_snapshots
         WHERE snapshot_ts <= p_max_timestamp
        GROUP BY 1
        HAVING count(1) > 1 -- Not useful to aggregate 1 snapshot !
        ORDER BY 1
        )
    LOOP
    
        RAISE INFO '%', l_aggr;
        
        -- Create new aggregate snapshot
        INSERT INTO histo_pgss_snapshots (snapshot_ts, snapshot_comment)
        VALUES (l_aggr.aggr_snapshot_ts, CONCAT('AGGR_', p_aggr_level, '_', l_aggr.aggr_snapshot_ts))
        RETURNING snapshot_id INTO l_snapshot_id
        ;

        l_snap_aggr_count := l_snap_aggr_count + 1;
        
        -- Insert aggregate history rows
        -- Almost all columns are summed, except min_time, max_time, mean_time
        -- stddev is averaged
        -- queryid is artificially maxed (who cares anyway ?)
        INSERT INTO histo_pgss_snapshot_details (snapshot_id, query_md5, userid, dbid
                                                ,queryid, query
                                                ,calls, total_time, min_time, max_time
                                                ,mean_time, stddev_time,"rows"
                                                ,shared_blks_hit,shared_blks_read,shared_blks_dirtied,shared_blks_written
                                                ,local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written
                                                ,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time)
        SELECT l_snapshot_id, query_md5, userid, dbid
             , MAX(queryid), query
             , SUM(calls), ROUND(SUM(total_time)::numeric,6), MIN(min_time), MAX(max_time)
             , ROUND((SUM(total_time)/SUM(calls))::numeric,6) mean_time, ROUND((SUM(stddev_time)/SUM(calls))::numeric,6) stddev_time, SUM("rows") 
             , SUM(shared_blks_hit), SUM(shared_blks_read), SUM(shared_blks_dirtied), SUM(shared_blks_written)
             , SUM(local_blks_hit), SUM(local_blks_read), SUM(local_blks_dirtied), SUM(local_blks_written)
             , SUM(temp_blks_read), SUM(temp_blks_written), SUM(blk_read_time), SUM(blk_write_time)
        FROM histo_pgss_snapshot_details
        WHERE snapshot_id = ANY(l_aggr.snapshot_array)
        GROUP BY l_snapshot_id, query_md5, userid, dbid, query
        ;
        
        -- Remove aggregated snapshots
        DELETE FROM histo_pgss_snapshots 
        WHERE snapshot_id = ANY(l_aggr.snapshot_array)
        ;
        GET DIAGNOSTICS l_nb_rows = ROW_COUNT;
        l_snap_count := l_snap_count + l_nb_rows;

    END LOOP;

    l_return = CONCAT(l_snap_count,' snapshot(s) removed; ', l_snap_aggr_count, ' created.');

    RETURN l_return;

END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION histo_pgss_aggregate(timestamptz,varchar) IS '
Aggregation of histo_pgss tables.
';

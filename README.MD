histo_pgss
=====

This simple extension keeps history of pg_stat_statements.


License
-------

This software is distributed under the GNU General Public License.


Objectives
----------

This extension keeps history of pg_stat_statements. The extension creates 2 tables and 3 functions to manage this history.

Tables : 
 * histo_pgss_snapshots : keeps tracks of snapshots (timestamps).
 * histo_pgss_snapshot_details : keeps history of statements with pg_stat_statements attributes.

Functions : 
 * histo_pgss_snapshot(varchar) : takes a snapshot, i.e. copies content of pg_stat_statements in histo_pgss_snapshot_details.
 * histo_pgss_purge(timestampz) : removes a selection of snapshots from histo_pgss tables
 * histo_pgss_aggregate(timestampz, varchar) : aggregates snapshots to a larger timescale

Distribution
------------

Not yet available from PGXN platform...


Installation
-------------

Of course pg_stat_statements extension needs to be installed beforehand. It is also better to install histo_pgss in the same schema as pg_stat_statements.

### Simple way

Just open a psql connection to your database and execute content of histo_pgss--1.0.sql

### As an extension

Download zip : https://github.com/ldechambe/histo_pgss/archive/master.zip

Then, in the folder containing the downloaded file : 
```
unzip histo_pgss-master.zip
cd histo_pgss-master
cp histo_pgss.control <SHAREDIR_DIRECTORY>/extension/.
cp sql/* <SHAREDIR_DIRECTORY>/extension/.
```
Now, connect to your database using a user able to create extensions :
```
# CREATE EXTENSION histo_pgss;
CREATE EXTENSION
```

Usage
-----

### Take a snapshot
To take a snapshot, just execute function histo_pgss_snapshot(), it returns the snapshot_id.
```
# SELECT count(1) FROM pg_stat_statements;
 count
-------
    473
(1 row)

# SELECT histo_pgss_snapshot();
 histo_pgss_snapshot
---------------------
                   1
(1 row)

# SELECT * FROM histo_pgss_snapshots;
 snapshot_id |          snapshot_ts          |       snapshot_comment
-------------+-------------------------------+-------------------------------
           1 | 2019-08-27 11:19:03.579683+02 | 2019-08-27 11:19:03.579683+02
(1 row)

# SELECT count(1) FROM histo_pgss_snapshot_details;
 count
-------
   474
(1 row)

# SELECT count(1) FROM pg_stat_statements;
 count
-------
      4
(1 row)
```
Table histo_pgss_snapshot_details will be fed with previous content of pg_stat_statements and pg_stat_statements will be emptied. 
Note that histo_pgss_snapshot() can accept an optional argument which will be stored in the snapshot_comment column.

### Schedule
Of course, instead of taking snapshots manually,which is useful to compare two batch jobs for instance, you might like to schedule. Here's an example with crontab : 
```
# Snapshot every hour
0 * * * *  psql -h localhost -d mypgdb -t -c "SELECT NOW(),'SNAPSHOT',histo_pgss_snapshot();" 1>>/path/to/log/histo_pgss.log 2>&1
```

### Purge
You can prevent histo_pgss_details to grow beyond control by using histo_pgss_purge. This will remove all snapshots taken before 2019-08-27 11:20:00 :
```
# SELECT histo_pgss_purge('2019-08-27 11:20:00');
    histo_pgss_purge
-------------------------
 1 snapshot(s) removed;
(1 row)
```
Argument is optional. By default the function will remove all snapshots older than 30 days.

### Aggregate
If you want to keep track of past statements on a long period without having a huge table, you can aggregate snapshots. You will end up with one big snapshot over a long period, aggregating values of snapshots on smaller periods. Note that former snapshots will be removed. The function histo_pgss_aggregate takes two arguments : 
 * max_timestamp : aggregate until this timestamp
 * aggregation level : any value from 'YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE' 
This will aggregate all snapshots before 2019-08-01 to a granularity of a day.
```
# SELECT histo_pgss_aggregate('2019-08-01 00:00:00','DAY');
       histo_pgss_aggregate
-----------------------------------
 375 snapshot(s) removed; 16 created.
(1 row)
```
Note that using this function will imply that snapshots will no longer be ordered by snapshot_id as new aggregate snapshots are created for old periods.

### queryid vs query_md5
In histo_pgss_snapshot_details you can find a query_md5 column. For some reason in pg_stat_statements, queries with identical texts might appear as separate entries (ie with different queryid). To avoid this a query_md5 column has been added.

See : https://www.postgresql.org/docs/current/pgstatstatements.html

### Query histo_pgss tables
Some examples below...

Number of queries captured by snapshot
```
SELECT s.snapshot_id,s.snapshot_ts,s.snapshot_comment,count(1)
FROM histo_pgss_snapshots s
JOIN histo_pgss_snapshot_details d on (s.snapshot_id=d.snapshot_id)
GROUP BY 1,2,3
ORDER BY 1 desc;
```

Top 5 longest queries, hour by hour on a specific day
```
WITH X AS (
SELECT DATE_TRUNC('HOUR', s.snapshot_ts) AS hour_, query_md5, query
     , SUM(total_time) AS total_time
     , RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', s.snapshot_ts) ORDER BY SUM(total_time) desc) AS rank_
FROM histo_pgss_snapshots s 
JOIN histo_pgss_snapshot_details d ON (d.snapshot_id=s.snapshot_id)
WHERE DATE_TRUNC('DAY',s.snapshot_ts) = '2019-08-14' 
GROUP BY 1,2,3
ORDER BY 1,4 DESC 
)
SELECT hour_, rank_, query_md5, total_time, query
FROM x
WHERE rank_ <=5 
ORDER BY 1,2
;
```
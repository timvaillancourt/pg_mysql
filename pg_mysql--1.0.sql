\echo Use "CREATE EXTENSION pg_mysql" to load this extension. \quit

CREATE TYPE mysql.start_result AS (
    port        integer,
    pid         integer,
    datadir     text
);

CREATE TYPE mysql.status_info AS (
    port            integer,
    pid             integer,
    is_running      boolean,
    datadir         text,
    role            text,
    source_uuid     text,
    replication_io  text,
    replication_sql text,
    seconds_behind  integer,
    semi_sync       boolean
);

CREATE FUNCTION mysql.start(port integer DEFAULT 0, semi_sync boolean DEFAULT false)
RETURNS mysql.start_result
AS 'MODULE_PATHNAME', 'pg_mysql_start'
LANGUAGE C VOLATILE;

CREATE FUNCTION mysql.start_replica(source_host text, source_port integer, port integer DEFAULT 0, semi_sync boolean DEFAULT false)
RETURNS mysql.start_result
AS 'MODULE_PATHNAME', 'pg_mysql_start_replica'
LANGUAGE C VOLATILE;

CREATE FUNCTION mysql.stop(port integer)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_mysql_stop'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION mysql.status(port integer DEFAULT 0)
RETURNS SETOF mysql.status_info
AS 'MODULE_PATHNAME', 'pg_mysql_status'
LANGUAGE C VOLATILE ROWS 10;

CREATE FUNCTION mysql.delete(port integer)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_mysql_delete'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION mysql.query(sql text, port integer DEFAULT 0)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_mysql_query'
LANGUAGE C VOLATILE;

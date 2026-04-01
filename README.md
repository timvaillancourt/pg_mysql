# pg_mysql

Production-grade replication for PostgreSQL - at last!

A PostgreSQL extension that launches and manages MySQL servers from within PostgreSQL. Start primaries, spin up read-only replicas with GTID replication, [Clone Plugin](https://dev.mysql.com/doc/refman/8.4/en/clone-plugin.html), and [semi-synchronous replication](https://dev.mysql.com/doc/refman/8.4/en/replication-semisync.html), and query MySQL — all from `psql`.

- _"this is a truly chaotic project"_ - Claude Opus 4.6 _(genuine response to my prompt)_
- _"interesting, what is the use case? Oh, wait, is this a joke?"_ - Eduardo, Slack Datastores

![demo](demo/demo.gif)

## Requirements

- PostgreSQL 17+
- MySQL 8.4+ (tested with Percona Server 8.4)
- `mysqld` and `mysql` binaries accessible on the system

## Build and Install

Install dependencies (macOS):

```bash
brew install postgresql@17 percona-server
```

Then build and install the extension:

```bash
make PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config
make install PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config
```

Restart PostgreSQL:

```bash
# macOS (Homebrew)
brew services restart postgresql@17

# Linux (systemd)
sudo systemctl restart postgresql
```

Then:

```sql
CREATE EXTENSION pg_mysql;
```

## Usage

### Start a primary

```sql
SELECT * FROM mysql.start();
```

Starts `mysqld` in the background with binary logging, GTIDs, and a random port in the 33061-34060 range. A specific port can be passed: `mysql.start(3307)`.

To enable [semi-synchronous replication](https://dev.mysql.com/doc/refman/8.4/en/replication-semisync.html):

```sql
SELECT * FROM mysql.start(semi_sync => true);
```

The data directory is auto-created at `<basedir>/<port>/data/` and initialized on first start.

### Start a replica

```sql
SELECT * FROM mysql.start_replica('127.0.0.1', 33722);
```

Spins up a new `mysqld`, clones all data from the source via Clone Plugin, then starts GTID-based replication in `super-read-only` mode. An optional third argument specifies the replica port.

To enable semi-synchronous replication on the replica (and source):

```sql
SELECT * FROM mysql.start_replica('127.0.0.1', 33722, semi_sync => true);
```

The source host can be any reachable MySQL server.

### Check status

```sql
SELECT * FROM mysql.status();
```

```
 port  |  pid  | is_running |         datadir          |  role   |             source_uuid              | replication_io | replication_sql | seconds_behind | semi_sync
-------+-------+------------+--------------------------+---------+--------------------------------------+----------------+-----------------+----------------+-----------
 33108 | 33544 | t          | /tmp/pg_mysql/33108/data | replica | 8a94f357-11ef-11e8-b642-0ed5f89f718b | Yes            | Yes             |              0 | t
 33722 | 33521 | t          | /tmp/pg_mysql/33722/data | primary | 8a94f357-11ef-11e8-b642-0ed5f89f718b |                |                 |                | t
```

Returns all instances sorted by port. Pass a port to query a single instance: `mysql.status(33722)`.

### Run queries

```sql
SELECT mysql.query('SHOW DATABASES');
SELECT mysql.query('SELECT * FROM mydb.t1', 33108);  -- specific port
```

Defaults to the primary when no port is given.

### Stop and delete

```sql
SELECT mysql.stop(33722);   -- stop instance by port
SELECT mysql.delete(33108); -- stop + remove data directory
```

Both require a port.

## Configuration

Superuser-only GUC variables, set in `postgresql.conf` or via `SET`:

| Variable | Default | Description |
|---|---|---|
| `pg_mysql.mysqld_path` | `/opt/homebrew/bin/mysqld` | Path to `mysqld` binary |
| `pg_mysql.mysql_path` | `/opt/homebrew/bin/mysql` | Path to `mysql` client binary |
| `pg_mysql.basedir` | `/tmp/pg_mysql` | Base directory for instance data (`<basedir>/<port>/`) |

## TODO

- [ ] Map `mysql.query()` input and responses to native PostgreSQL types and result sets
- [ ] Understand the code Claude wrote

## License

Apache License 2.0

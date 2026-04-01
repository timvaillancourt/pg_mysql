MODULES = pg_mysql
EXTENSION = pg_mysql
DATA = pg_mysql--1.0.sql
PGFILEDESC = "pg_mysql - launch a MySQL server from within PostgreSQL"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

CREATE DATABASE vc_demo;

\connect vc_demo

CREATE SCHEMA example;

COMMENT ON SCHEMA example IS 'Version control demo database schema';

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

SET search_path = example, pg_catalog;

CREATE TABLE IF NOT EXISTS test_relation (
	a int not null,
	b varchar(64) not null,
	c timestamp default now(),
	d float,
	CONSTRAINT test_relation_pk PRIMARY KEY(a, b)
);

INSERT INTO test_relation
	SELECT series, random()::varchar(64), now(), random()::float
	FROM generate_series(1, 10) series;
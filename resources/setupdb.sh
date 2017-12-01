#!/usr/bin/env bash

~/dev/SeniorProject/pgsql/bin/dropdb test_db
~/dev/SeniorProject/pgsql/bin/createdb test_db

~/dev/SeniorProject/pgsql/bin/psql -f createdb.sql -d test_db

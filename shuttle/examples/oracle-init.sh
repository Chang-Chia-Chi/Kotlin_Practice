#!/usr/bin/env bash
# Applies schema.sql (spec 8.1's DDL, verbatim) as the application user, once, at the database's first start.
#
# Two reasons this is a shell script rather than schema.sql dropped straight into initdb.d:
#   1. gvenzl runs a plain .sql there as SYSDBA, so the tables would belong to SYS, not to the user shuttle
#      connects as;
#   2. SQL*Plus does not end a statement on a `;` that has a `-- comment` after it on the same line, and the
#      spec's DDL has exactly that on its first line - so `CREATE SEQUENCE file_transfer_seq` silently runs
#      into the next statement and both sequences fail with ORA-03405. Stripping comments fixes it, and
#      SQLBLANKLINES ON keeps the now-blank comment lines inside CREATE TABLE from ending the statement early.
set -euo pipefail

{
  echo "SET SQLBLANKLINES ON"
  echo "WHENEVER SQLERROR EXIT SQL.SQLCODE"
  sed 's/--.*$//' /schema/schema.sql
  echo "EXIT"
} | sqlplus -s "${APP_USER}/${APP_USER_PASSWORD}@//localhost:1521/FREEPDB1"

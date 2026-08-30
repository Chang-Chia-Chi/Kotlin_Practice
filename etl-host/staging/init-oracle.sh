#!/bin/bash
# Everything the staging stack needs Oracle to hold, and nothing the application creates itself.
#
# A .sh rather than a .sql, and that is measured rather than stylistic. gvenzl/oracle-free runs
# every file in /container-entrypoint-initdb.d once - but a bare .sql runs **as SYS in CDB$ROOT**,
# not as the application user in the pluggable database. The first version of this file was a
# .sql, the container reported "DONE: running init.sql" with no error, and the application then
# failed both startup refreshes with ORA-00942 against tables that did exist - in the wrong
# container, owned by the wrong user. Connecting explicitly is what makes "where did this land"
# unambiguous.
set -euo pipefail

sqlplus -s -L "${APP_USER}/${APP_USER_PASSWORD}@//localhost:1521/FREEPDB1" <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE

-- 1. The source tables behind etl-host.cache.sql's two groups. Each statement in that property
--    MUST project an `id` column: VerifyConfig.keyUnique defaults to true and the verify gate
--    runs COUNT(id), COUNT(DISTINCT id) over every table of a candidate. Without one the first
--    refresh fails and the pod never becomes ready, with the real cause two systems away.
CREATE TABLE lot (
  id      NUMBER(18),
  lot_id  VARCHAR2(40),
  qty     NUMBER(18, 3),
  site    VARCHAR2(8)
);

INSERT INTO lot
SELECT LEVEL, 'L' || LEVEL, LEVEL * 1.5,
       CASE WHEN MOD(LEVEL, 2) = 0 THEN 'F12' ELSE 'F11' END
  FROM dual CONNECT BY LEVEL <= 500;

CREATE TABLE equipment (
  id      NUMBER(18),
  tool_id VARCHAR2(40),
  state   VARCHAR2(8)
);

INSERT INTO equipment
SELECT LEVEL, 'T' || LEVEL,
       CASE WHEN MOD(LEVEL, 2) = 0 THEN 'UP' ELSE 'DOWN' END
  FROM dual CONNECT BY LEVEL <= 40;

-- 2. The task file's pipe target. Outside scratch a `pipe` step defaults to
--    createTable: REQUIRED (SimpleEtl spec 4.4), so the table must already exist. The framework
--    fills it BY COLUMN NAME from catalog metadata, so the column order here need not match the
--    YAML's select list. NUMBER(18) is declared rather than left to an expression: an uncast
--    expression reports precision 0 and AUTO DDL rejects it at writer open.
CREATE TABLE wip_summary (
  site      VARCHAR2(8),
  lots      NUMBER(18),
  total_qty NUMBER(38, 3)
);

-- 3. The archive layer's manifest, verbatim from ManifestSchema.DDL. Applied here rather than by
--    the application on purpose: the layer creates neither of its prerequisites, because a table
--    or a bucket made by whichever pod started first is exactly the ambient side effect its
--    ordering guarantees exist to avoid.
--
--    TIMESTAMP WITH TIME ZONE, not bare TIMESTAMP: the column is read back by a different process
--    than wrote it, and a bare one round-trips through whatever zone each JVM happens to have -
--    which would silently shift every data_as_of the watermark predicate depends on.
CREATE TABLE SNAPSHOT_ARCHIVE_MANIFEST (
  group_id   VARCHAR2(128)            NOT NULL,
  version    NUMBER(19)               NOT NULL,
  data_as_of TIMESTAMP WITH TIME ZONE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  uri_prefix VARCHAR2(512)            NOT NULL,
  inventory  CLOB                     NOT NULL,
  status     VARCHAR2(16)             NOT NULL,
  generation NUMBER(19)               NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  CONSTRAINT snapshot_archive_manifest_pk PRIMARY KEY (group_id, version),
  CONSTRAINT snapshot_archive_status_ck CHECK (status IN ('PENDING', 'COMPLETE', 'FAILED'))
);

CREATE INDEX snapshot_archive_watermark_ix
    ON SNAPSHOT_ARCHIVE_MANIFEST (group_id, status, data_as_of);

-- NOCACHE because one version per group per hour makes sequence-cache throughput irrelevant,
-- while a cache is what lets numbers come back out of order after a restart - and MAX(version)
-- is how a watermark is chosen.
CREATE SEQUENCE SNAPSHOT_ARCHIVE_VERSION_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;

COMMIT;
EXIT
SQL

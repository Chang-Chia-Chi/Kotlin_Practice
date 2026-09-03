package infra.shuttle.jdbi

/** Spec 8.1, verbatim. The DBA applies it (D15); tests apply it to a container; the code only reads it. */
object StateStoreSchema {
    val DDL: String = """
CREATE SEQUENCE file_transfer_seq;                     -- ids; the store reads NEXTVAL
CREATE SEQUENCE delivery_outbox_seq;

CREATE TABLE file_transfer (
  id                NUMBER(19)     NOT NULL,
  route             VARCHAR2(64)   NOT NULL,
  parent_id         NUMBER(19),                        -- set on child rows
  kind              VARCHAR2(16)   NOT NULL,           -- OBJECT | MESSAGE | CHILD
  source_kind       VARCHAR2(16)   NOT NULL,           -- SFTP | S3 | NATS
  source_ref        VARCHAR2(1024) NOT NULL,           -- store + directory, or channel + subject
  source_name       VARCHAR2(512)  NOT NULL,           -- file name, or message id
  source_size       NUMBER(19),
  source_mtime      TIMESTAMP,
  revision          NUMBER(5)      DEFAULT 1 NOT NULL,  -- next value when the same identity returns with different content
  supersedes_id     NUMBER(19),                        -- the finished row this revision replaces
  source_digest     VARCHAR2(128),
  digest            VARCHAR2(128),
  digest_algo       VARCHAR2(16),
  stored_name       VARCHAR2(512),
  stored_mtime      TIMESTAMP,
  state             VARCHAR2(16)   NOT NULL,
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  last_error        VARCHAR2(2000),
  attributes        CLOB,                              -- JSON map, bounded (rule 22)
  target_kind       VARCHAR2(16),
  target_location   VARCHAR2(255),                     -- bucket, or host + directory
  target_key        VARCHAR2(1024),
  target_ref        VARCHAR2(512),                     -- adapter-defined; S3: the version id
  target_size       NUMBER(19),
  first_seen_at     TIMESTAMP      NOT NULL,
  updated_at        TIMESTAMP      NOT NULL,
  acked_at          TIMESTAMP,
  completed_at      TIMESTAMP,
  CONSTRAINT pk_file_transfer PRIMARY KEY (id),
  CONSTRAINT fk_file_transfer_parent FOREIGN KEY (parent_id) REFERENCES file_transfer (id),
  CONSTRAINT fk_file_transfer_supersedes FOREIGN KEY (supersedes_id) REFERENCES file_transfer (id),
  CONSTRAINT uq_file_transfer_identity UNIQUE (route, source_ref, source_name, source_size, source_mtime, revision)
);
CREATE INDEX ix_file_transfer_state  ON file_transfer (route, state, updated_at);
CREATE INDEX ix_file_transfer_parent ON file_transfer (parent_id);

CREATE TABLE delivery_outbox (
  id                NUMBER(19)     NOT NULL,
  file_transfer_id  NUMBER(19)     NOT NULL,
  on_state          VARCHAR2(16)   NOT NULL,           -- the moment this notification announces, fixed for ever: FETCHED | STORED | ACKED
  channel           VARCHAR2(64)   NOT NULL,
  notification_state VARCHAR2(16)  NOT NULL,           -- the notification's own progress: PENDING | DELIVERED | FAILED
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  next_attempt_at   TIMESTAMP      NOT NULL,
  last_status       VARCHAR2(64),
  last_error        VARCHAR2(2000),
  reference         VARCHAR2(255),                     -- the id the channel returned for the delivered call
  created_at        TIMESTAMP      NOT NULL,
  delivered_at      TIMESTAMP,
  CONSTRAINT pk_delivery_outbox PRIMARY KEY (id),
  CONSTRAINT fk_delivery_transfer FOREIGN KEY (file_transfer_id) REFERENCES file_transfer (id),
  CONSTRAINT uq_delivery_on_state_channel UNIQUE (file_transfer_id, on_state, channel)
);
CREATE INDEX ix_delivery_due ON delivery_outbox (notification_state, next_attempt_at);
""".trimStart('\n')

    /** The DDL as the JDBC driver wants it: one statement per call, comments and blank lines dropped. */
    fun statements(): List<String> = DDL.lines()
        .map { it.substringBefore("--").trimEnd() }
        .joinToString("\n")
        .split(";")
        .map { it.trim() }
        .filter { it.isNotEmpty() }
}

-- dispatch_batch: tracks batch runs in prod
CREATE TABLE dispatch_batch (
    batch_token  VARCHAR2(64)   NOT NULL,
    status       VARCHAR2(16)   NOT NULL,
    created_at   TIMESTAMP      NOT NULL,
    config_count NUMBER,
    CONSTRAINT pk_dispatch_batch PRIMARY KEY (batch_token),
    CONSTRAINT chk_dispatch_batch_status CHECK (status IN ('NORMAL', 'DRYRUN'))
);

CREATE INDEX idx_dispatch_batch_status_created ON dispatch_batch (status, created_at);

-- dispatch_event: stores dispatch decisions in prod
CREATE TABLE dispatch_event (
    id             NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_token    VARCHAR2(64)  NOT NULL,
    config_id      VARCHAR2(64)  NOT NULL,
    dispatch_order NUMBER        NOT NULL,
    product_id     VARCHAR2(64)  NOT NULL,
    source_bom_id  VARCHAR2(64)  NOT NULL,
    qty            NUMBER        NOT NULL,
    target_site_id VARCHAR2(64)  NOT NULL,
    target_bom_id  VARCHAR2(64),
    site_gap       NUMBER        NOT NULL,
    bom_gap        NUMBER,
    CONSTRAINT pk_dispatch_event PRIMARY KEY (id),
    CONSTRAINT fk_dispatch_event_batch FOREIGN KEY (batch_token) REFERENCES dispatch_batch (batch_token)
);

CREATE INDEX idx_dispatch_event_batch_config ON dispatch_event (batch_token, config_id);
CREATE INDEX idx_dispatch_event_config_batch ON dispatch_event (config_id, batch_token);

-- dispatch_batch_stg: tracks batch runs in stg (identical schema)
CREATE TABLE dispatch_batch_stg (
    batch_token  VARCHAR2(64)   NOT NULL,
    status       VARCHAR2(16)   NOT NULL,
    created_at   TIMESTAMP      NOT NULL,
    config_count NUMBER,
    CONSTRAINT pk_dispatch_batch_stg PRIMARY KEY (batch_token),
    CONSTRAINT chk_dispatch_batch_stg_status CHECK (status IN ('NORMAL', 'DRYRUN'))
);

CREATE INDEX idx_dispatch_batch_stg_status_created ON dispatch_batch_stg (status, created_at);

-- dispatch_event_stg: stores dispatch decisions in stg (identical schema)
CREATE TABLE dispatch_event_stg (
    id             NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_token    VARCHAR2(64)  NOT NULL,
    config_id      VARCHAR2(64)  NOT NULL,
    dispatch_order NUMBER        NOT NULL,
    product_id     VARCHAR2(64)  NOT NULL,
    source_bom_id  VARCHAR2(64)  NOT NULL,
    qty            NUMBER        NOT NULL,
    target_site_id VARCHAR2(64)  NOT NULL,
    target_bom_id  VARCHAR2(64),
    site_gap       NUMBER        NOT NULL,
    bom_gap        NUMBER,
    CONSTRAINT pk_dispatch_event_stg PRIMARY KEY (id),
    CONSTRAINT fk_dispatch_event_stg_batch FOREIGN KEY (batch_token) REFERENCES dispatch_batch_stg (batch_token)
);

CREATE INDEX idx_dispatch_event_stg_batch_config ON dispatch_event_stg (batch_token, config_id);
CREATE INDEX idx_dispatch_event_stg_config_batch ON dispatch_event_stg (config_id, batch_token);

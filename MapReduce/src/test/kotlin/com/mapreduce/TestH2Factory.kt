package com.mapreduce

import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin

/**
 * Shared H2 JDBI factory for repository-level tests.
 *
 * Provides an in-memory H2 database with Oracle compatibility mode and
 * the full schema applied. Each call to [create] produces an isolated
 * JDBI instance (unique DB name) so tests never interfere.
 */
object TestH2Factory {

    private var counter = 0

    fun create(): Jdbi {
        val dbName = "testdb_${counter++}_${System.nanoTime()}"
        val jdbi = Jdbi.create(
            "jdbc:h2:mem:$dbName;MODE=Oracle;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=TRUE",
            "sa", ""
        )
        jdbi.installPlugin(KotlinPlugin())
        jdbi.useHandle<Exception> { h ->
            h.execute(SCHEMA)
        }
        return jdbi
    }

    private val SCHEMA = """
        CREATE TABLE task (
            task_id            VARCHAR(36)   NOT NULL PRIMARY KEY,
            handler            VARCHAR(255)  NOT NULL,
            queue              VARCHAR(100)  DEFAULT 'default' NOT NULL,
            payload            CLOB,
            status             VARCHAR(20)   DEFAULT 'PENDING' NOT NULL,
            priority           INT           DEFAULT 0 NOT NULL,
            group_id           VARCHAR(36),
            metadata           CLOB,
            claimed_by         VARCHAR(255),
            claimed_at         TIMESTAMP,
            scheduled_at       TIMESTAMP,
            retry_count        INT           DEFAULT 0 NOT NULL,
            max_retries        INT           DEFAULT 3 NOT NULL,
            error_message      VARCHAR(4000),
            created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
            completed_at       TIMESTAMP,
            execution_generation VARCHAR(36),
            speculative        INT           DEFAULT 0 NOT NULL,
            last_heartbeat     TIMESTAMP,
            last_epoch         BIGINT        DEFAULT 0 NOT NULL
        );

        CREATE TABLE mr_job (
            job_id              VARCHAR(36)   NOT NULL PRIMARY KEY,
            job_type            VARCHAR(255)  NOT NULL,
            status              VARCHAR(20)   NOT NULL,
            job_params          CLOB,
            total_tasks         INT           DEFAULT 0 NOT NULL,
            completed_tasks     INT           DEFAULT 0 NOT NULL,
            failed_tasks        INT           DEFAULT 0 NOT NULL,
            failure_policy      VARCHAR(20)   DEFAULT 'FAIL_JOB' NOT NULL,
            failure_threshold   DECIMAL(5,4)  DEFAULT 0 NOT NULL,
            reducing_fence_token VARCHAR(255),
            result_metadata     CLOB,
            total_partitions    INT           DEFAULT 1 NOT NULL,
            last_epoch          BIGINT        DEFAULT 0 NOT NULL,
            version             BIGINT        DEFAULT 0 NOT NULL,
            created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL
        );

        CREATE TABLE mr_output (
            output_id       VARCHAR(36)   NOT NULL PRIMARY KEY,
            job_id          VARCHAR(36)   NOT NULL,
            task_id         VARCHAR(36)   NOT NULL,
            output_data     CLOB,
            blob_uri        VARCHAR(2000),
            partition_hash  INT           DEFAULT 0 NOT NULL,
            created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
            CONSTRAINT fk_mr_output_job FOREIGN KEY (job_id) REFERENCES mr_job (job_id)
        );
    """.trimIndent()
}

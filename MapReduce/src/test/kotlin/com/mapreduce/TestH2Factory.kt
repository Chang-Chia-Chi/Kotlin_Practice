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
            step_id            VARCHAR(36),
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
            last_epoch         BIGINT        DEFAULT 0 NOT NULL,
            output_uri         VARCHAR(1000),
            output_metadata    CLOB
        );

        CREATE TABLE workflow_step (
            step_id             VARCHAR(36)   NOT NULL PRIMARY KEY,
            workflow_name       VARCHAR(255)  NOT NULL,
            run_id              VARCHAR(36)   NOT NULL,
            status              VARCHAR(20)   NOT NULL,
            params              CLOB,
            queue               VARCHAR(100)  DEFAULT 'default' NOT NULL,
            step_label          VARCHAR(50)   NOT NULL,
            step_total          INT           DEFAULT 0 NOT NULL,
            tasks_pending       INT           DEFAULT 0 NOT NULL,
            tasks_failed        INT           DEFAULT 0 NOT NULL,
            on_complete_handler VARCHAR(255),
            failure_policy      VARCHAR(20)   DEFAULT 'FAIL_STEP' NOT NULL,
            failure_threshold   DECIMAL(5,4)  DEFAULT 0 NOT NULL,
            result_metadata     CLOB,
            version             BIGINT        DEFAULT 0 NOT NULL,
            last_epoch          BIGINT        DEFAULT 0 NOT NULL,
            deadline_at         TIMESTAMP,
            created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
            UNIQUE (workflow_name, run_id, step_label)
        );

        CREATE INDEX idx_wf_step_run ON workflow_step (run_id);
    """.trimIndent()
}

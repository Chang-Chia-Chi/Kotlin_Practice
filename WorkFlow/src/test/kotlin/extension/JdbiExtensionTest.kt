package com.workflow.extension

import com.workflow.engine.OracleTestContainer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbiExtensionTest {

    private lateinit var jdbi: Jdbi

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        // Create a test-only scratch table for transaction tests
        jdbi.useHandle<Exception> { handle ->
            handle.execute(
                """
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE jdbi_ext_test PURGE';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN RAISE; END IF;
                END;
                """
            )
            handle.execute(
                "CREATE TABLE jdbi_ext_test (id VARCHAR2(36) NOT NULL, val VARCHAR2(100))"
            )
        }
    }

    @AfterEach
    fun cleanTable() {
        jdbi.useHandle<Exception> { it.execute("DELETE FROM jdbi_ext_test") }
    }

    // ── R0.3 — NonCancellable transaction wrappers ───────────────────────

    @Nested
    inner class InTransactionSuspendTests {

        @Test
        fun `inTransactionSuspend completes despite parent cancellation`() = runTest {
            val txnCompleted = AtomicBoolean(false)
            val id = "txn-cancel-${System.nanoTime()}"

            try {
                coroutineScope {
                    val deferred = async {
                        jdbi.inTransactionSuspend<String, Exception> { handle ->
                            handle.execute(
                                "INSERT INTO jdbi_ext_test (id, val) VALUES (?, ?)",
                                id,
                                "committed",
                            )
                            txnCompleted.set(true)
                            id
                        }
                    }
                    // Let the transaction start on the IO dispatcher
                    delay(50)
                    // Cancel the coroutine scope while transaction is in-flight
                    cancel("test cancellation")
                    // deferred.await() would throw CancellationException
                }
            } catch (_: CancellationException) {
                // expected
            }

            assertTrue(txnCompleted.get(), "Transaction callback should run to completion under NonCancellable")

            // Verify the row was actually committed to the database
            val row = jdbi.withHandle<String?, Exception> { handle ->
                handle.createQuery("SELECT val FROM jdbi_ext_test WHERE id = :id")
                    .bind("id", id)
                    .mapTo(String::class.java)
                    .findOne()
                    .orElse(null)
            }
            assertNotNull(row, "Row should exist — transaction committed despite cancellation")
            assertEquals("committed", row)
        }

        @Test
        fun `inTransactionSuspend returns value on normal completion`() = runTest {
            val id = "normal-${System.nanoTime()}"

            val result = jdbi.inTransactionSuspend<String, Exception> { handle ->
                handle.execute(
                    "INSERT INTO jdbi_ext_test (id, val) VALUES (?, ?)",
                    id,
                    "value",
                )
                "returned"
            }

            assertEquals("returned", result)
            val row = jdbi.withHandle<String?, Exception> { handle ->
                handle.createQuery("SELECT val FROM jdbi_ext_test WHERE id = :id")
                    .bind("id", id)
                    .mapTo(String::class.java)
                    .findOne()
                    .orElse(null)
            }
            assertEquals("value", row)
        }
    }

    @Nested
    inner class UseTransactionSuspendTests {

        @Test
        fun `useTransactionSuspend completes despite parent cancellation`() = runTest {
            val txnCompleted = AtomicBoolean(false)
            val id = "use-txn-cancel-${System.nanoTime()}"

            try {
                coroutineScope {
                    launch {
                        jdbi.useTransactionSuspend<Exception> { handle ->
                            handle.execute(
                                "INSERT INTO jdbi_ext_test (id, val) VALUES (?, ?)",
                                id,
                                "use-committed",
                            )
                            txnCompleted.set(true)
                        }
                    }
                    delay(50)
                    cancel("test cancellation")
                }
            } catch (_: CancellationException) {
                // expected
            }

            assertTrue(txnCompleted.get(), "useTransactionSuspend should run to completion under NonCancellable")

            val row = jdbi.withHandle<String?, Exception> { handle ->
                handle.createQuery("SELECT val FROM jdbi_ext_test WHERE id = :id")
                    .bind("id", id)
                    .mapTo(String::class.java)
                    .findOne()
                    .orElse(null)
            }
            assertNotNull(row, "Row should exist — useTransaction committed despite cancellation")
            assertEquals("use-committed", row)
        }
    }

    @Nested
    inner class WithHandleSuspendTests {

        @Test
        fun `withHandleSuspend is cancellable - does NOT use NonCancellable`() = runTest {
            // withHandleSuspend uses plain withContext(IO) — no NonCancellable.
            // When the parent scope is already cancelled before dispatch, the
            // withContext should throw CancellationException immediately.
            val wasCancelled = AtomicBoolean(false)

            try {
                coroutineScope {
                    launch {
                        // Cancel from inside this scope
                        this@coroutineScope.cancel("pre-cancel")
                        // Now try withHandleSuspend — should observe cancellation
                        try {
                            jdbi.withHandleSuspend<String, Exception> { handle ->
                                handle.createQuery("SELECT 1 FROM DUAL")
                                    .mapTo(Int::class.java).one()
                                "should not reach"
                            }
                        } catch (_: CancellationException) {
                            wasCancelled.set(true)
                            throw CancellationException("propagated")
                        }
                    }
                }
            } catch (_: CancellationException) {
                // expected
            }

            assertTrue(
                wasCancelled.get(),
                "withHandleSuspend should be cancellable — CancellationException should propagate",
            )
        }

        @Test
        fun `inTransactionSuspend survives pre-cancelled scope via NonCancellable`() = runTest {
            // Contrast: inTransactionSuspend uses NonCancellable, so it should
            // still execute even when the scope is already cancelled.
            val txnCompleted = AtomicBoolean(false)
            val id = "pre-cancel-txn-${System.nanoTime()}"

            try {
                coroutineScope {
                    launch {
                        this@coroutineScope.cancel("pre-cancel")
                        // NonCancellable means this still runs
                        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
                            handle.execute(
                                "INSERT INTO jdbi_ext_test (id, val) VALUES (?, ?)",
                                id,
                                "survived",
                            )
                            txnCompleted.set(true)
                        }
                    }
                }
            } catch (_: CancellationException) {
                // expected
            }

            assertTrue(
                txnCompleted.get(),
                "inTransactionSuspend should complete despite pre-cancelled scope",
            )

            val row = jdbi.withHandle<String?, Exception> { handle ->
                handle.createQuery("SELECT val FROM jdbi_ext_test WHERE id = :id")
                    .bind("id", id)
                    .mapTo(String::class.java)
                    .findOne()
                    .orElse(null)
            }
            assertEquals("survived", row)
        }

        @Test
        fun `withHandleSuspend returns value on normal completion`() = runTest {
            val result = jdbi.withHandleSuspend<Int, Exception> { handle ->
                handle.createQuery("SELECT 42 FROM DUAL")
                    .mapTo(Int::class.java)
                    .one()
            }

            assertEquals(42, result)
        }
    }
}

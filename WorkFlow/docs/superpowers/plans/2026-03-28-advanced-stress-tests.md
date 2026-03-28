# Advanced Stress Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SQL-layer fault injection, throughput benchmarks, and a lightweight history checker to the stress test suite -- inspired by MIT 6.824, hashicorp/raft, MicroRaft, and Jepsen testing patterns.

**Architecture:** Three phases: (1) A JDBC `DataSource` wrapper that intercepts SQL execution to inject typed faults (fail, delay, empty results) by regex pattern matching. (2) A timing harness that measures workflow throughput and per-workflow latency percentiles. (3) A `TransitionHandler` decorator that records execution events for post-hoc property checking (no duplicate execution, monotonic phases, no lost tasks).

**Tech Stack:** Kotlin, JUnit 5, JDBI 3, Oracle Testcontainers, Toxiproxy, Awaitility, HikariCP

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/test/kotlin/stress/FaultInjector.kt` | FaultInjector, FaultRule, FaultInjectingDataSource — SQL-level fault injection |
| Create | `src/test/kotlin/stress/FaultInjectionStressTest.kt` | F1-F6: typed fault injection test scenarios |
| Modify | `src/test/kotlin/stress/StressTestBase.kt` | Wire FaultInjector into DataSource chain, add reset to @AfterEach |
| Create | `src/test/kotlin/stress/BenchmarkHarness.kt` | BenchmarkResult, percentile calculation, formatted output |
| Create | `src/test/kotlin/stress/ThroughputBenchmarkTest.kt` | B1-B5: throughput benchmark scenarios |
| Create | `src/test/kotlin/stress/HistoryRecorder.kt` | HistoryEvent, HistoryRecorder, HistoryChecker — post-hoc property verification |
| Modify | `src/test/kotlin/stress/CorrectnessStressTest.kt` | Add HistoryRecorder to C1 |
| Modify | `src/test/kotlin/stress/IdempotencyStressTest.kt` | Add HistoryRecorder to I1, I7 |
| Modify | `src/test/kotlin/stress/FaultInjectionStressTest.kt` | Add HistoryRecorder to F4, F6 |

---

## Task 1: Create FaultInjector Core

**Files:**
- Create: `src/test/kotlin/stress/FaultInjector.kt`

- [ ] **Step 1: Create FaultInjector, FaultRule, and FaultInjectingDataSource**

Create `src/test/kotlin/stress/FaultInjector.kt`:

```kotlin
package com.workflow.stress

import java.io.PrintWriter
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import javax.sql.DataSource

/**
 * SQL-level fault injection for stress tests.
 *
 * Wraps a JDBC DataSource and intercepts PreparedStatement execution.
 * Registered fault rules match SQL patterns and inject failures, delays,
 * or empty results — enabling typed fault injection that Toxiproxy
 * (network-level) cannot provide.
 *
 * Inspired by MicroRaft's typed message filtering and etcd/raft's
 * pure state machine testability.
 */
class FaultInjector {

    private val rules = CopyOnWriteArrayList<FaultRule>()

    fun onSql(pattern: String): FaultRule {
        val rule = FaultRule(Regex(pattern))
        rules.add(rule)
        return rule
    }

    fun reset() {
        rules.clear()
    }

    internal fun checkBeforeExecute(sql: String) {
        for (rule in rules) {
            if (rule.pattern.containsMatchIn(sql)) {
                rule.applyBeforeExecute(sql)
            }
        }
    }

    internal fun shouldReturnEmpty(sql: String): Boolean {
        for (rule in rules) {
            if (rule.pattern.containsMatchIn(sql)) {
                if (rule.tryConsumeEmpty()) return true
            }
        }
        return false
    }
}

class FaultRule(internal val pattern: Regex) {

    private var failRemaining = AtomicInteger(0)
    private var failException: SQLException = SQLException("injected fault")
    private var delayDuration: Duration? = null
    private var emptyRemaining = AtomicInteger(0)
    private var failOnNth: Int = 0
    private var failNthException: SQLException = SQLException("injected fault")
    private val executionCount = AtomicInteger(0)

    fun failNext(times: Int = 1, exception: SQLException = SQLException("injected fault")): FaultRule {
        failRemaining.set(times)
        failException = exception
        return this
    }

    fun delay(duration: Duration): FaultRule {
        delayDuration = duration
        return this
    }

    fun returnEmpty(times: Int = 1): FaultRule {
        emptyRemaining.set(times)
        return this
    }

    fun failNth(n: Int, exception: SQLException = SQLException("injected fault")): FaultRule {
        failOnNth = n
        failNthException = exception
        return this
    }

    internal fun applyBeforeExecute(sql: String) {
        // Delay
        val delay = delayDuration
        if (delay != null) {
            Thread.sleep(delay.toMillis())
        }

        // Fail next N
        if (failRemaining.getAndUpdate { if (it > 0) it - 1 else 0 } > 0) {
            throw failException
        }

        // Fail Nth execution
        if (failOnNth > 0) {
            val count = executionCount.incrementAndGet()
            if (count == failOnNth) {
                throw failNthException
            }
        }
    }

    internal fun tryConsumeEmpty(): Boolean =
        emptyRemaining.getAndUpdate { if (it > 0) it - 1 else 0 } > 0
}

/**
 * DataSource wrapper that routes connections through FaultInjector.
 */
class FaultInjectingDataSource(
    private val delegate: DataSource,
    private val injector: FaultInjector,
) : DataSource {

    override fun getConnection(): Connection =
        FaultInjectingConnection(delegate.connection, injector)

    override fun getConnection(username: String?, password: String?): Connection =
        FaultInjectingConnection(delegate.getConnection(username, password), injector)

    override fun getLogWriter(): PrintWriter? = delegate.logWriter
    override fun setLogWriter(out: PrintWriter?) { delegate.logWriter = out }
    override fun setLoginTimeout(seconds: Int) { delegate.loginTimeout = seconds }
    override fun getLoginTimeout(): Int = delegate.loginTimeout
    override fun getParentLogger(): Logger = delegate.parentLogger
    override fun <T : Any?> unwrap(iface: Class<T>?): T = delegate.unwrap(iface)
    override fun isWrapperFor(iface: Class<*>?): Boolean = delegate.isWrapperFor(iface)
}

/**
 * Connection wrapper that creates fault-injecting PreparedStatements.
 */
private class FaultInjectingConnection(
    private val delegate: Connection,
    private val injector: FaultInjector,
) : Connection by delegate {

    override fun prepareStatement(sql: String): PreparedStatement =
        FaultInjectingPreparedStatement(delegate.prepareStatement(sql), sql, injector)

    override fun prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement =
        FaultInjectingPreparedStatement(delegate.prepareStatement(sql, autoGeneratedKeys), sql, injector)

    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement =
        FaultInjectingPreparedStatement(
            delegate.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, injector,
        )

    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): PreparedStatement =
        FaultInjectingPreparedStatement(
            delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), sql, injector,
        )

    override fun prepareStatement(sql: String, columnNames: Array<out String>?): PreparedStatement =
        FaultInjectingPreparedStatement(delegate.prepareStatement(sql, columnNames), sql, injector)

    override fun prepareStatement(sql: String, columnIndexes: IntArray?): PreparedStatement =
        FaultInjectingPreparedStatement(delegate.prepareStatement(sql, columnIndexes), sql, injector)
}

/**
 * PreparedStatement wrapper that checks fault rules before execution.
 */
private class FaultInjectingPreparedStatement(
    private val delegate: PreparedStatement,
    private val sql: String,
    private val injector: FaultInjector,
) : PreparedStatement by delegate {

    override fun execute(): Boolean {
        injector.checkBeforeExecute(sql)
        if (injector.shouldReturnEmpty(sql)) return false
        return delegate.execute()
    }

    override fun executeQuery(): ResultSet {
        injector.checkBeforeExecute(sql)
        if (injector.shouldReturnEmpty(sql)) return EmptyResultSet(delegate.executeQuery().metaData)
        return delegate.executeQuery()
    }

    override fun executeUpdate(): Int {
        injector.checkBeforeExecute(sql)
        return delegate.executeUpdate()
    }

    override fun executeBatch(): IntArray {
        injector.checkBeforeExecute(sql)
        return delegate.executeBatch()
    }
}

/**
 * ResultSet that is always empty. Used by returnEmpty() fault rule.
 * Delegates metadata to the real result set's metadata for column info.
 */
private class EmptyResultSet(private val meta: ResultSetMetaData?) : ResultSet {
    override fun next(): Boolean = false
    override fun close() {}
    override fun wasNull(): Boolean = true
    override fun getMetaData(): ResultSetMetaData? = meta

    // All getters throw since next() is always false
    override fun getString(columnIndex: Int): String = throw SQLException("No current row")
    override fun getString(columnLabel: String?): String = throw SQLException("No current row")
    override fun getInt(columnIndex: Int): Int = throw SQLException("No current row")
    override fun getInt(columnLabel: String?): Int = throw SQLException("No current row")
    override fun getLong(columnIndex: Int): Long = throw SQLException("No current row")
    override fun getLong(columnLabel: String?): Long = throw SQLException("No current row")
    override fun getObject(columnIndex: Int): Any = throw SQLException("No current row")
    override fun getObject(columnLabel: String?): Any = throw SQLException("No current row")

    // Navigation — all return false/empty
    override fun isBeforeFirst(): Boolean = false
    override fun isAfterLast(): Boolean = true
    override fun isFirst(): Boolean = false
    override fun isLast(): Boolean = false
    override fun first(): Boolean = false
    override fun last(): Boolean = false
    override fun getRow(): Int = 0
    override fun previous(): Boolean = false
    override fun beforeFirst() {}
    override fun afterLast() {}
    override fun absolute(row: Int): Boolean = false
    override fun relative(rows: Int): Boolean = false
    override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
    override fun setFetchDirection(direction: Int) {}
    override fun getFetchSize(): Int = 0
    override fun setFetchSize(rows: Int) {}
    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY
    override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY
    override fun getHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
    override fun isClosed(): Boolean = false
    override fun getStatement(): java.sql.Statement? = null
    override fun findColumn(columnLabel: String?): Int = throw SQLException("No current row")
    override fun rowUpdated(): Boolean = false
    override fun rowInserted(): Boolean = false
    override fun rowDeleted(): Boolean = false
    override fun getCursorName(): String = throw SQLException("No current row")
    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper")
    override fun isWrapperFor(iface: Class<*>?): Boolean = false

    // Remaining getters — all throw
    override fun getBoolean(columnIndex: Int): Boolean = throw SQLException("No current row")
    override fun getBoolean(columnLabel: String?): Boolean = throw SQLException("No current row")
    override fun getByte(columnIndex: Int): Byte = throw SQLException("No current row")
    override fun getByte(columnLabel: String?): Byte = throw SQLException("No current row")
    override fun getShort(columnIndex: Int): Short = throw SQLException("No current row")
    override fun getShort(columnLabel: String?): Short = throw SQLException("No current row")
    override fun getFloat(columnIndex: Int): Float = throw SQLException("No current row")
    override fun getFloat(columnLabel: String?): Float = throw SQLException("No current row")
    override fun getDouble(columnIndex: Int): Double = throw SQLException("No current row")
    override fun getDouble(columnLabel: String?): Double = throw SQLException("No current row")
    @Deprecated("Use getBigDecimal without scale")
    override fun getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = throw SQLException("No current row")
    override fun getBigDecimal(columnIndex: Int): java.math.BigDecimal = throw SQLException("No current row")
    @Deprecated("Use getBigDecimal without scale")
    override fun getBigDecimal(columnLabel: String?, scale: Int): java.math.BigDecimal = throw SQLException("No current row")
    override fun getBigDecimal(columnLabel: String?): java.math.BigDecimal = throw SQLException("No current row")
    override fun getBytes(columnIndex: Int): ByteArray = throw SQLException("No current row")
    override fun getBytes(columnLabel: String?): ByteArray = throw SQLException("No current row")
    override fun getDate(columnIndex: Int): java.sql.Date = throw SQLException("No current row")
    override fun getDate(columnLabel: String?): java.sql.Date = throw SQLException("No current row")
    override fun getDate(columnIndex: Int, cal: java.util.Calendar?): java.sql.Date = throw SQLException("No current row")
    override fun getDate(columnLabel: String?, cal: java.util.Calendar?): java.sql.Date = throw SQLException("No current row")
    override fun getTime(columnIndex: Int): java.sql.Time = throw SQLException("No current row")
    override fun getTime(columnLabel: String?): java.sql.Time = throw SQLException("No current row")
    override fun getTime(columnIndex: Int, cal: java.util.Calendar?): java.sql.Time = throw SQLException("No current row")
    override fun getTime(columnLabel: String?, cal: java.util.Calendar?): java.sql.Time = throw SQLException("No current row")
    override fun getTimestamp(columnIndex: Int): java.sql.Timestamp = throw SQLException("No current row")
    override fun getTimestamp(columnLabel: String?): java.sql.Timestamp = throw SQLException("No current row")
    override fun getTimestamp(columnIndex: Int, cal: java.util.Calendar?): java.sql.Timestamp = throw SQLException("No current row")
    override fun getTimestamp(columnLabel: String?, cal: java.util.Calendar?): java.sql.Timestamp = throw SQLException("No current row")
    override fun getAsciiStream(columnIndex: Int): java.io.InputStream = throw SQLException("No current row")
    override fun getAsciiStream(columnLabel: String?): java.io.InputStream = throw SQLException("No current row")
    @Deprecated("Use getCharacterStream")
    override fun getUnicodeStream(columnIndex: Int): java.io.InputStream = throw SQLException("No current row")
    @Deprecated("Use getCharacterStream")
    override fun getUnicodeStream(columnLabel: String?): java.io.InputStream = throw SQLException("No current row")
    override fun getBinaryStream(columnIndex: Int): java.io.InputStream = throw SQLException("No current row")
    override fun getBinaryStream(columnLabel: String?): java.io.InputStream = throw SQLException("No current row")
    override fun getWarnings(): java.sql.SQLWarning? = null
    override fun clearWarnings() {}
    override fun getCharacterStream(columnIndex: Int): java.io.Reader = throw SQLException("No current row")
    override fun getCharacterStream(columnLabel: String?): java.io.Reader = throw SQLException("No current row")
    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any = throw SQLException("No current row")
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any = throw SQLException("No current row")
    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T = throw SQLException("No current row")
    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T = throw SQLException("No current row")
    override fun getRef(columnIndex: Int): java.sql.Ref = throw SQLException("No current row")
    override fun getRef(columnLabel: String?): java.sql.Ref = throw SQLException("No current row")
    override fun getBlob(columnIndex: Int): java.sql.Blob = throw SQLException("No current row")
    override fun getBlob(columnLabel: String?): java.sql.Blob = throw SQLException("No current row")
    override fun getClob(columnIndex: Int): java.sql.Clob = throw SQLException("No current row")
    override fun getClob(columnLabel: String?): java.sql.Clob = throw SQLException("No current row")
    override fun getArray(columnIndex: Int): java.sql.Array = throw SQLException("No current row")
    override fun getArray(columnLabel: String?): java.sql.Array = throw SQLException("No current row")
    override fun getURL(columnIndex: Int): java.net.URL = throw SQLException("No current row")
    override fun getURL(columnLabel: String?): java.net.URL = throw SQLException("No current row")
    override fun getRowId(columnIndex: Int): java.sql.RowId = throw SQLException("No current row")
    override fun getRowId(columnLabel: String?): java.sql.RowId = throw SQLException("No current row")
    override fun getNClob(columnIndex: Int): java.sql.NClob = throw SQLException("No current row")
    override fun getNClob(columnLabel: String?): java.sql.NClob = throw SQLException("No current row")
    override fun getSQLXML(columnIndex: Int): java.sql.SQLXML = throw SQLException("No current row")
    override fun getSQLXML(columnLabel: String?): java.sql.SQLXML = throw SQLException("No current row")
    override fun getNString(columnIndex: Int): String = throw SQLException("No current row")
    override fun getNString(columnLabel: String?): String = throw SQLException("No current row")
    override fun getNCharacterStream(columnIndex: Int): java.io.Reader = throw SQLException("No current row")
    override fun getNCharacterStream(columnLabel: String?): java.io.Reader = throw SQLException("No current row")

    // Update methods — all throw
    override fun updateNull(columnIndex: Int) = throw SQLException("Read-only")
    override fun updateNull(columnLabel: String?) = throw SQLException("Read-only")
    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw SQLException("Read-only")
    override fun updateBoolean(columnLabel: String?, x: Boolean) = throw SQLException("Read-only")
    override fun updateByte(columnIndex: Int, x: Byte) = throw SQLException("Read-only")
    override fun updateByte(columnLabel: String?, x: Byte) = throw SQLException("Read-only")
    override fun updateShort(columnIndex: Int, x: Short) = throw SQLException("Read-only")
    override fun updateShort(columnLabel: String?, x: Short) = throw SQLException("Read-only")
    override fun updateInt(columnIndex: Int, x: Int) = throw SQLException("Read-only")
    override fun updateInt(columnLabel: String?, x: Int) = throw SQLException("Read-only")
    override fun updateLong(columnIndex: Int, x: Long) = throw SQLException("Read-only")
    override fun updateLong(columnLabel: String?, x: Long) = throw SQLException("Read-only")
    override fun updateFloat(columnIndex: Int, x: Float) = throw SQLException("Read-only")
    override fun updateFloat(columnLabel: String?, x: Float) = throw SQLException("Read-only")
    override fun updateDouble(columnIndex: Int, x: Double) = throw SQLException("Read-only")
    override fun updateDouble(columnLabel: String?, x: Double) = throw SQLException("Read-only")
    override fun updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal?) = throw SQLException("Read-only")
    override fun updateBigDecimal(columnLabel: String?, x: java.math.BigDecimal?) = throw SQLException("Read-only")
    override fun updateString(columnIndex: Int, x: String?) = throw SQLException("Read-only")
    override fun updateString(columnLabel: String?, x: String?) = throw SQLException("Read-only")
    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw SQLException("Read-only")
    override fun updateBytes(columnLabel: String?, x: ByteArray?) = throw SQLException("Read-only")
    override fun updateDate(columnIndex: Int, x: java.sql.Date?) = throw SQLException("Read-only")
    override fun updateDate(columnLabel: String?, x: java.sql.Date?) = throw SQLException("Read-only")
    override fun updateTime(columnIndex: Int, x: java.sql.Time?) = throw SQLException("Read-only")
    override fun updateTime(columnLabel: String?, x: java.sql.Time?) = throw SQLException("Read-only")
    override fun updateTimestamp(columnIndex: Int, x: java.sql.Timestamp?) = throw SQLException("Read-only")
    override fun updateTimestamp(columnLabel: String?, x: java.sql.Timestamp?) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: java.io.InputStream?, length: Int) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Int) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: java.io.InputStream?, length: Int) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: java.io.Reader?, length: Int) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, x: java.io.Reader?, length: Int) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, x: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw SQLException("Read-only")
    override fun updateObject(columnIndex: Int, x: Any?) = throw SQLException("Read-only")
    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) = throw SQLException("Read-only")
    override fun updateObject(columnLabel: String?, x: Any?) = throw SQLException("Read-only")
    override fun insertRow() = throw SQLException("Read-only")
    override fun updateRow() = throw SQLException("Read-only")
    override fun deleteRow() = throw SQLException("Read-only")
    override fun refreshRow() = throw SQLException("Read-only")
    override fun cancelRowUpdates() = throw SQLException("Read-only")
    override fun moveToInsertRow() = throw SQLException("Read-only")
    override fun moveToCurrentRow() = throw SQLException("Read-only")
    override fun updateRef(columnIndex: Int, x: java.sql.Ref?) = throw SQLException("Read-only")
    override fun updateRef(columnLabel: String?, x: java.sql.Ref?) = throw SQLException("Read-only")
    override fun updateBlob(columnIndex: Int, x: java.sql.Blob?) = throw SQLException("Read-only")
    override fun updateBlob(columnLabel: String?, x: java.sql.Blob?) = throw SQLException("Read-only")
    override fun updateBlob(columnIndex: Int, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateBlob(columnLabel: String?, x: java.io.InputStream?, length: Long) = throw SQLException("Read-only")
    override fun updateBlob(columnIndex: Int, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateBlob(columnLabel: String?, x: java.io.InputStream?) = throw SQLException("Read-only")
    override fun updateClob(columnIndex: Int, x: java.sql.Clob?) = throw SQLException("Read-only")
    override fun updateClob(columnLabel: String?, x: java.sql.Clob?) = throw SQLException("Read-only")
    override fun updateClob(columnIndex: Int, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateClob(columnLabel: String?, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateClob(columnIndex: Int, x: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateClob(columnLabel: String?, x: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateArray(columnIndex: Int, x: java.sql.Array?) = throw SQLException("Read-only")
    override fun updateArray(columnLabel: String?, x: java.sql.Array?) = throw SQLException("Read-only")
    override fun updateRowId(columnIndex: Int, x: java.sql.RowId?) = throw SQLException("Read-only")
    override fun updateRowId(columnLabel: String?, x: java.sql.RowId?) = throw SQLException("Read-only")
    override fun updateNString(columnIndex: Int, nString: String?) = throw SQLException("Read-only")
    override fun updateNString(columnLabel: String?, nString: String?) = throw SQLException("Read-only")
    override fun updateNClob(columnIndex: Int, nClob: java.sql.NClob?) = throw SQLException("Read-only")
    override fun updateNClob(columnLabel: String?, nClob: java.sql.NClob?) = throw SQLException("Read-only")
    override fun updateNClob(columnIndex: Int, reader: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateNClob(columnLabel: String?, reader: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateNClob(columnIndex: Int, reader: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateNClob(columnLabel: String?, reader: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateSQLXML(columnIndex: Int, xmlObject: java.sql.SQLXML?) = throw SQLException("Read-only")
    override fun updateSQLXML(columnLabel: String?, xmlObject: java.sql.SQLXML?) = throw SQLException("Read-only")
    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateNCharacterStream(columnLabel: String?, x: java.io.Reader?, length: Long) = throw SQLException("Read-only")
    override fun updateNCharacterStream(columnIndex: Int, x: java.io.Reader?) = throw SQLException("Read-only")
    override fun updateNCharacterStream(columnLabel: String?, x: java.io.Reader?) = throw SQLException("Read-only")
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/FaultInjector.kt
git commit -m "test: add SQL-level fault injection infrastructure for stress tests"
```

---

## Task 2: Wire FaultInjector into StressTestBase

**Files:**
- Modify: `src/test/kotlin/stress/StressTestBase.kt`

- [ ] **Step 1: Add FaultInjector field and wire into DataSource chain**

In `StressTestBase`, add the `faultInjector` field after the existing `proxyDataSource` field:

```kotlin
    protected val faultInjector = FaultInjector()
```

In `initInfrastructure()`, change line 146 from:

```kotlin
        proxyJdbi = Jdbi.create(proxyDataSource)
```

to:

```kotlin
        proxyJdbi = Jdbi.create(FaultInjectingDataSource(proxyDataSource, faultInjector))
```

- [ ] **Step 2: Add faultInjector.reset() to @AfterEach cleanUp**

In the `cleanUp()` method, add after the toxic reset block:

```kotlin
        // Reset fault injection rules
        faultInjector.reset()
```

- [ ] **Step 3: Verify existing tests still pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dgroups=stress -pl . -q`
Expected: All 41 stress tests PASS (fault injector is transparent when no rules registered)

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/stress/StressTestBase.kt
git commit -m "test: wire FaultInjector into StressTestBase DataSource chain"
```

---

## Task 3: Create FaultInjectionStressTest (F1-F6)

**Files:**
- Create: `src/test/kotlin/stress/FaultInjectionStressTest.kt`

- [ ] **Step 1: Create FaultInjectionStressTest with F1-F6**

Create `src/test/kotlin/stress/FaultInjectionStressTest.kt`:

```kotlin
package com.workflow.stress

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.workflow
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.sql.SQLException
import java.time.Duration

@Tag("stress")
class FaultInjectionStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- F1: CAS deadlock during phase advance ----

    @Test
    fun `F1 - CAS deadlock during phase advance - sweeper retries and recovers`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f1.handler") }
                activity("step2") { transition("f1.handler") }
            }
            val wfId = engine.startWorkflow(def, """{"test":"F1"}""")
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f1.handler", PassThroughHandler())
            startWorkerPool()

            // Fail the next CAS update on workflow version — simulates deadlock
            faultInjector.onSql("UPDATE workflow.*version").failNext(1, SQLException("ORA-00060: deadlock detected"))

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- F2: Full task contention — all tasks locked by other workers ----

    @Test
    fun `F2 - full task contention - workers back off then claim after rules expire`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f2.handler") }
            }
            val wfId = engine.startWorkflow(def, """{"test":"F2"}""")
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f2.handler", PassThroughHandler())

            // First 3 claim attempts return no tasks (simulates all locked by others)
            faultInjector.onSql("FOR UPDATE SKIP LOCKED").returnEmpty(3)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- F3: Slow INSERT during fan-out scatter ----

    @Test
    fun `F3 - slow INSERT during fan-out - completes correctly despite delay`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("scatter") {
                    transition("f3.scatter")
                    fanOut {
                        transition("f3.parallel")
                    }
                }
            }

            handlerRegistry.register("f3.scatter", object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    val payloads = (1..10).map { """{"item":$it}""" }
                    return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
                }
            })
            handlerRegistry.register("f3.parallel", PassThroughHandler())

            // Slow down task INSERT by 3 seconds (simulates slow disk during scatter)
            faultInjector.onSql("INSERT INTO task").delay(Duration.ofSeconds(3))

            val wfId = engine.startWorkflow(def, """{"test":"F3"}""")
            diagnostics.trackedWorkflows.add(wfId)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()

            // Verify all 10 parallel tasks were created correctly
            val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
            kotlin.test.assertEquals(10, parallelTasks.size)
        }

    // ---- F4: Partial commit — task UPDATE ok, workflow CAS fails ----

    @Test
    fun `F4 - partial commit - task completes but CAS fails - sweeper recovers`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f4.handler") }
                activity("step2") { transition("f4.handler") }
            }
            val wfId = engine.startWorkflow(def, """{"test":"F4"}""")
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f4.handler", PassThroughHandler())

            // Fail the 2nd SQL execution matching workflow version update within a transaction.
            // 1st execution = task status update (succeeds), 2nd = workflow CAS advance (fails).
            faultInjector.onSql("UPDATE workflow.*version").failNth(1, SQLException("ORA-00060: simulated partial commit"))

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- F5: Intermittent barrier stale read ----

    @Test
    fun `F5 - barrier stale read - recovers on subsequent sweep`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") {
                    transition("f5.handler")
                    retries(1)
                    failurePolicy(FailurePolicy.ABORT)
                }
            }
            val wfId = engine.startWorkflow(def, """{"test":"F5"}""")
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f5.handler", PassThroughHandler())

            // First barrier COUNT query returns empty — simulates stale MVCC snapshot
            faultInjector.onSql("SELECT COUNT.*task").returnEmpty(1)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()
        }

    // ---- F6: Deadlock storm then recovery ----

    @Test
    fun `F6 - deadlock storm then recovery - system converges after storm`() =
        runBlocking(Dispatchers.Default) {
            val batchSize = scale.workflowBatchSize
            val def = workflow {
                activity("step1") { transition("f6.handler") }
                activity("step2") { transition("f6.handler") }
            }

            handlerRegistry.register("f6.handler", PassThroughHandler())

            val wfIds = (1..batchSize).map {
                engine.startWorkflow(def, """{"test":"F6-$it"}""").also {
                    diagnostics.trackedWorkflows.add(it)
                }
            }

            startWorkerPool()

            // Storm: all CAS updates fail for next 20 attempts
            faultInjector.onSql("UPDATE workflow.*version").failNext(20, SQLException("ORA-00060: deadlock storm"))

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            // Wait for storm to clear (rules auto-expire after 20 failures)
            delay(5000)

            // System should converge — all workflows complete
            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
            }
            sweepJob.cancel()
        }
}
```

- [ ] **Step 2: Run fault injection tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="FaultInjectionStressTest" -pl . -q`
Expected: All 6 tests PASS

- [ ] **Step 3: Run all stress tests to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dgroups=stress -pl . -q`
Expected: All 47 stress tests PASS (41 existing + 6 new)

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/stress/FaultInjectionStressTest.kt
git commit -m "test: add FaultInjectionStressTest F1-F6 SQL-level fault scenarios"
```

---

## Task 4: Create BenchmarkHarness

**Files:**
- Create: `src/test/kotlin/stress/BenchmarkHarness.kt`

- [ ] **Step 1: Create BenchmarkResult and BenchmarkHarness**

Create `src/test/kotlin/stress/BenchmarkHarness.kt`:

```kotlin
package com.workflow.stress

import java.time.Duration
import java.time.Instant

/**
 * Throughput benchmark result with percentile latency calculations.
 *
 * Results are printed, not asserted — machine variance makes absolute
 * thresholds brittle. Use for relative comparison across runs.
 *
 * Inspired by MIT 6.824's repeated-apply-under-load pattern and
 * Jepsen's Kafka workload throughput measurement.
 */
data class BenchmarkResult(
    val label: String,
    val totalWorkflows: Int,
    val totalTasks: Int,
    val wallClockMs: Long,
    val latencies: List<Long>,
) {
    val workflowsPerSec: Double = if (wallClockMs > 0) totalWorkflows * 1000.0 / wallClockMs else 0.0
    val tasksPerSec: Double = if (wallClockMs > 0) totalTasks * 1000.0 / wallClockMs else 0.0
    val p50ms: Long = percentile(50)
    val p95ms: Long = percentile(95)
    val p99ms: Long = percentile(99)

    private fun percentile(p: Int): Long {
        if (latencies.isEmpty()) return 0
        val sorted = latencies.sorted()
        val index = ((p / 100.0) * sorted.size).toInt().coerceIn(0, sorted.size - 1)
        return sorted[index]
    }

    fun print() {
        println("=== $label ===")
        println("  Workflows: $totalWorkflows | Tasks: $totalTasks | Wall clock: ${wallClockMs}ms")
        println("  Throughput: ${"%.1f".format(workflowsPerSec)} wf/s | ${"%.1f".format(tasksPerSec)} tasks/s")
        println("  Latency: p50=${p50ms}ms  p95=${p95ms}ms  p99=${p99ms}ms")
        println()
    }
}

/**
 * Tracks workflow submission and completion times for benchmark measurement.
 */
class BenchmarkHarness {

    private val submissions = mutableMapOf<String, Instant>()
    private val completions = mutableMapOf<String, Instant>()

    fun recordSubmission(workflowId: String) {
        submissions[workflowId] = Instant.now()
    }

    fun recordCompletion(workflowId: String) {
        completions[workflowId] = Instant.now()
    }

    fun result(label: String, tasksPerWorkflow: Int): BenchmarkResult {
        val latencies = submissions.keys.mapNotNull { wfId ->
            val start = submissions[wfId] ?: return@mapNotNull null
            val end = completions[wfId] ?: return@mapNotNull null
            Duration.between(start, end).toMillis()
        }
        val wallClock = if (submissions.isNotEmpty() && completions.isNotEmpty()) {
            Duration.between(
                submissions.values.min(),
                completions.values.max(),
            ).toMillis()
        } else {
            0L
        }
        return BenchmarkResult(
            label = label,
            totalWorkflows = submissions.size,
            totalTasks = submissions.size * tasksPerWorkflow,
            wallClockMs = wallClock,
            latencies = latencies,
        )
    }

    fun reset() {
        submissions.clear()
        completions.clear()
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/BenchmarkHarness.kt
git commit -m "test: add BenchmarkHarness for throughput measurement with percentile latency"
```

---

## Task 5: Create ThroughputBenchmarkTest (B1-B5)

**Files:**
- Create: `src/test/kotlin/stress/ThroughputBenchmarkTest.kt`

- [ ] **Step 1: Create ThroughputBenchmarkTest with B1-B5**

Create `src/test/kotlin/stress/ThroughputBenchmarkTest.kt`:

```kotlin
package com.workflow.stress

import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.workflow
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import eu.rekawek.toxiproxy.model.ToxicDirection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Tag("benchmark")
class ThroughputBenchmarkTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- B1: Single-activity throughput ----

    @Test
    fun `B1 - single activity throughput`() = runBlocking(Dispatchers.Default) {
        val n = scale.fanOutSize // 50 in MODERATE
        val def = workflow {
            activity("step1") { transition("b1.handler") }
        }

        handlerRegistry.register("b1.handler", PassThroughHandler())

        val harness = BenchmarkHarness()

        val wfIds = (1..n).map {
            val wfId = engine.startWorkflow(def, """{"test":"B1-$it"}""")
            harness.recordSubmission(wfId)
            diagnostics.trackedWorkflows.add(wfId)
            wfId
        }

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            harness.recordCompletion(wfId)
        }
        sweepJob.cancel()

        harness.result("B1: Single-Activity Throughput", tasksPerWorkflow = 1).print()
    }

    // ---- B2: Fan-out/join throughput ----

    @Test
    fun `B2 - fan-out join throughput`() = runBlocking(Dispatchers.Default) {
        val n = scale.workflowBatchSize // 5 in MODERATE
        val fanOut = scale.fanOutSize   // 50 in MODERATE
        val def = workflow {
            activity("scatter") {
                transition("b2.scatter")
                fanOut {
                    transition("b2.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
            activity("final") { transition("b2.final") }
        }

        handlerRegistry.register("b2.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..fanOut).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("b2.parallel", PassThroughHandler())
        handlerRegistry.register("b2.final", PassThroughHandler())

        val harness = BenchmarkHarness()

        val wfIds = (1..n).map {
            val wfId = engine.startWorkflow(def, """{"test":"B2-$it"}""")
            harness.recordSubmission(wfId)
            diagnostics.trackedWorkflows.add(wfId)
            wfId
        }

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            harness.recordCompletion(wfId)
        }
        sweepJob.cancel()

        // tasksPerWorkflow = 1 scatter + fanOut parallel + 1 final
        harness.result("B2: Fan-Out/Join Throughput", tasksPerWorkflow = 1 + fanOut + 1).print()
    }

    // ---- B3: Multi-phase pipeline throughput ----

    @Test
    fun `B3 - multi-phase pipeline throughput`() = runBlocking(Dispatchers.Default) {
        val n = scale.workflowBatchSize
        val def = workflow {
            activity("phase1") { transition("b3.handler") }
            activity("phase2") { transition("b3.handler") }
            activity("phase3") { transition("b3.handler") }
            activity("phase4") { transition("b3.handler") }
            activity("phase5") { transition("b3.handler") }
        }

        handlerRegistry.register("b3.handler", PassThroughHandler())

        val harness = BenchmarkHarness()

        val wfIds = (1..n).map {
            val wfId = engine.startWorkflow(def, """{"test":"B3-$it"}""")
            harness.recordSubmission(wfId)
            diagnostics.trackedWorkflows.add(wfId)
            wfId
        }

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            harness.recordCompletion(wfId)
        }
        sweepJob.cancel()

        harness.result("B3: Multi-Phase Pipeline (5 phases)", tasksPerWorkflow = 5).print()
    }

    // ---- B4: Throughput under network fault ----

    @Test
    fun `B4 - throughput under network latency`() = runBlocking(Dispatchers.Default) {
        val n = scale.fanOutSize
        val def = workflow {
            activity("step1") { transition("b4.handler"); retries(3) }
        }

        handlerRegistry.register("b4.handler", PassThroughHandler())

        val harness = BenchmarkHarness()

        val wfIds = (1..n).map {
            val wfId = engine.startWorkflow(def, """{"test":"B4-$it"}""")
            harness.recordSubmission(wfId)
            diagnostics.trackedWorkflows.add(wfId)
            wfId
        }

        startWorkerPool()

        // Inject 500ms latency after workflows are submitted
        oracleProxy.toxics().latency("slow-b4", ToxicDirection.DOWNSTREAM, 500)

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            harness.recordCompletion(wfId)
        }
        sweepJob.cancel()

        harness.result("B4: Throughput Under 500ms Network Latency", tasksPerWorkflow = 1).print()
    }

    // ---- B5: Sweep overhead at scale ----

    @Test
    fun `B5 - sweep overhead at scale`() = runBlocking(Dispatchers.Default) {
        val n = 100
        val def = workflow {
            activity("step1") { transition("b5.handler") }
            activity("step2") { transition("b5.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)

        handlerRegistry.register("b5.handler", PassThroughHandler())

        // Create N stuck workflows via direct SQL (no workers needed for setup)
        val wfIds = (1..n).map { i ->
            val wfId = randomId()
            diagnostics.trackedWorkflows.add(wfId)
            insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
            insertTaskDirect(
                workflowId = wfId,
                sequenceNumber = 1,
                status = "COMPLETED",
                handlerKey = "b5.handler",
                result = """{"test":"B5-$i"}""",
            )
            updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))
            wfId
        }

        // Start workers for step2
        startWorkerPool()

        val harness = BenchmarkHarness()
        wfIds.forEach { harness.recordSubmission(it) }

        // Measure sweep + recovery time
        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            harness.recordCompletion(wfId)
        }
        sweepJob.cancel()

        harness.result("B5: Sweep Overhead ($n stuck workflows)", tasksPerWorkflow = 2).print()
    }
}
```

- [ ] **Step 2: Run benchmark tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ThroughputBenchmarkTest" -pl .`
Expected: All 5 tests PASS with benchmark output printed

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/ThroughputBenchmarkTest.kt
git commit -m "test: add ThroughputBenchmarkTest B1-B5 throughput benchmark scenarios"
```

---

## Task 6: Create HistoryRecorder and HistoryChecker

**Files:**
- Create: `src/test/kotlin/stress/HistoryRecorder.kt`

- [ ] **Step 1: Create HistoryRecorder, HistoryEvent, HistoryChecker**

Create `src/test/kotlin/stress/HistoryRecorder.kt`:

```kotlin
package com.workflow.stress

import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.fail

/**
 * Event types recorded during handler execution.
 */
enum class EventType {
    EXECUTE_START,
    EXECUTE_END,
    EXECUTE_FAIL,
}

/**
 * A single recorded event from handler execution history.
 */
data class HistoryEvent(
    val taskId: String,
    val workflowId: String,
    val thread: String,
    val timestamp: Instant,
    val type: EventType,
)

/**
 * TransitionHandler decorator that records execution events.
 *
 * Wrap any handler to capture a timeline of task executions
 * for post-hoc property verification via [HistoryChecker].
 *
 * Inspired by Jepsen's operation history recording and
 * Porcupine's linearizability checker input format.
 */
class HistoryRecorder(
    private val delegate: TransitionHandler,
) : TransitionHandler {

    val events = ConcurrentLinkedQueue<HistoryEvent>()

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val thread = Thread.currentThread().name
        events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_START))
        return try {
            val output = delegate.execute(input)
            events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_END))
            output
        } catch (e: Exception) {
            events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_FAIL))
            throw e
        }
    }
}

/**
 * Post-hoc property checker for handler execution histories.
 *
 * Checks properties inspired by Jepsen/Maelstrom Kafka workload:
 * no lost tasks, no duplicates, monotonic progression.
 */
object HistoryChecker {

    /**
     * Verifies no task was successfully executed more than once.
     * Detects: SKIP LOCKED failure, double-claim, stale reclaim race.
     */
    fun noDuplicateExecution(events: List<HistoryEvent>): List<String> {
        val completions = events.filter { it.type == EventType.EXECUTE_END }
        val byTask = completions.groupBy { it.taskId }
        return byTask.filter { it.value.size > 1 }.map { (taskId, executions) ->
            "DUPLICATE_EXECUTION: task $taskId executed ${executions.size} times on threads: ${executions.map { it.thread }}"
        }
    }

    /**
     * Verifies every task that was started eventually reached a terminal DB state.
     * Requires final DB task state for comparison.
     *
     * @param events recorded handler events
     * @param dbTasks final task rows from DB (maps with STATUS key)
     */
    fun noLostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<String> {
        val startedTaskIds = events.filter { it.type == EventType.EXECUTE_START }.map { it.taskId }.toSet()
        val terminalStatuses = setOf("COMPLETED", "FAILED", "DEAD_LETTER", "TIMED_OUT")
        val dbTaskMap = dbTasks.associateBy { it["ID"]?.toString() ?: "" }

        return startedTaskIds.mapNotNull { taskId ->
            val dbTask = dbTaskMap[taskId]
            if (dbTask == null) {
                "LOST_TASK: task $taskId was executed but not found in DB"
            } else {
                val status = dbTask["STATUS"]?.toString()
                if (status !in terminalStatuses) {
                    "LOST_TASK: task $taskId was executed but stuck in status $status"
                } else {
                    null
                }
            }
        }
    }

    /**
     * Verifies every EXECUTE_END event has a matching DB row in COMPLETED or FAILED status.
     */
    fun noGhostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<String> {
        val completedTaskIds = events.filter { it.type == EventType.EXECUTE_END }.map { it.taskId }.toSet()
        val dbTaskMap = dbTasks.associateBy { it["ID"]?.toString() ?: "" }

        return completedTaskIds.mapNotNull { taskId ->
            val dbTask = dbTaskMap[taskId]
            if (dbTask == null) {
                "GHOST_TASK: task $taskId completed in handler but not found in DB"
            } else {
                null
            }
        }
    }

    /**
     * Assert all checks pass. Fails the test with details if any violation found.
     */
    fun assertNoDuplicateExecution(events: List<HistoryEvent>) {
        val violations = noDuplicateExecution(events)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }

    fun assertNoLostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>) {
        val violations = noLostTasks(events, dbTasks)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }

    fun assertNoGhostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>) {
        val violations = noGhostTasks(events, dbTasks)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/HistoryRecorder.kt
git commit -m "test: add HistoryRecorder and HistoryChecker for post-hoc property verification"
```

---

## Task 7: Add HistoryRecorder to Existing Stress Tests

**Files:**
- Modify: `src/test/kotlin/stress/CorrectnessStressTest.kt` (C1)
- Modify: `src/test/kotlin/stress/IdempotencyStressTest.kt` (I1, I7)
- Modify: `src/test/kotlin/stress/FaultInjectionStressTest.kt` (F4, F6)

- [ ] **Step 1: Add HistoryRecorder to C1 in CorrectnessStressTest**

In `CorrectnessStressTest`, modify the `C1` test. Replace the handler registration:

```kotlin
        handlerRegistry.register("c1.parallel", PassThroughHandler())
```

with:

```kotlin
        val recorder = HistoryRecorder(PassThroughHandler())
        handlerRegistry.register("c1.parallel", recorder)
```

Add after the existing `assertNoTaskDuplicates` call, before `sweepJob.cancel()`:

```kotlin
        HistoryChecker.assertNoDuplicateExecution(recorder.events.toList())
```

- [ ] **Step 2: Add HistoryRecorder to I1 in IdempotencyStressTest**

In `IdempotencyStressTest`, modify the `I1` test. Replace:

```kotlin
        handlerRegistry.register("i1.handler", PassThroughHandler())
```

with:

```kotlin
        val recorder = HistoryRecorder(PassThroughHandler())
        handlerRegistry.register("i1.handler", recorder)
```

Add before `sweepJob.cancel()`:

```kotlin
        HistoryChecker.assertNoDuplicateExecution(recorder.events.toList())
```

- [ ] **Step 3: Add HistoryRecorder to I7 in IdempotencyStressTest**

In `IdempotencyStressTest`, modify the `I7` test. Replace:

```kotlin
        val counting = CountingHandler()
        handlerRegistry.register("i7.handler", counting)
```

with:

```kotlin
        val counting = CountingHandler()
        val recorder = HistoryRecorder(counting)
        handlerRegistry.register("i7.handler", recorder)
```

Add before `sweepJob.cancel()`:

```kotlin
        HistoryChecker.assertNoDuplicateExecution(recorder.events.toList())
```

- [ ] **Step 4: Add HistoryRecorder to F4 in FaultInjectionStressTest**

In `FaultInjectionStressTest`, modify the `F4` test. Replace:

```kotlin
            handlerRegistry.register("f4.handler", PassThroughHandler())
```

with:

```kotlin
            val recorder = HistoryRecorder(PassThroughHandler())
            handlerRegistry.register("f4.handler", recorder)
```

Add before `sweepJob.cancel()`:

```kotlin
            val allTasks = readTasksDirect(wfId)
            HistoryChecker.assertNoLostTasks(recorder.events.toList(), allTasks)
```

- [ ] **Step 5: Add HistoryRecorder to F6 in FaultInjectionStressTest**

In `FaultInjectionStressTest`, modify the `F6` test. Replace:

```kotlin
            handlerRegistry.register("f6.handler", PassThroughHandler())
```

with:

```kotlin
            val recorder = HistoryRecorder(PassThroughHandler())
            handlerRegistry.register("f6.handler", recorder)
```

Add after the `for (wfId in wfIds)` assertion loop, before `sweepJob.cancel()`:

```kotlin
            val allTasks = wfIds.flatMap { readTasksDirect(it) }
            HistoryChecker.assertNoLostTasks(recorder.events.toList(), allTasks)
```

- [ ] **Step 6: Run all modified tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CorrectnessStressTest,IdempotencyStressTest,FaultInjectionStressTest" -pl . -q`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/test/kotlin/stress/CorrectnessStressTest.kt src/test/kotlin/stress/IdempotencyStressTest.kt src/test/kotlin/stress/FaultInjectionStressTest.kt
git commit -m "test: integrate HistoryChecker into C1, I1, I7, F4, F6 for post-hoc verification"
```

---

## Task 8: Full Test Suite Verification

**Files:** None (verification only)

- [ ] **Step 1: Run all stress tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dgroups=stress -pl .`
Expected: All 47 stress tests PASS (41 existing + 6 fault injection)

- [ ] **Step 2: Run benchmark tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dgroups=benchmark -pl .`
Expected: All 5 benchmark tests PASS with throughput output printed

- [ ] **Step 3: Run full test suite for regression check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All tests PASS

# DynaCache P1 — Data Engine + Single Node

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-node Redis-compatible cache server. When this phase is done, `redis-cli` can connect and run all supported commands — String, Hash, List, Sorted Set, TTL, MULTI/EXEC, and Lua scripts.

**Architecture:** Standalone Maven multi-module project at `~/GitHub/Kotlin_Practice/DynaCache/`. The engine module is pure Kotlin (no I/O). The server module wires Netty (RESP) to the engine. Data structures are hand-built (skip list, timer wheel, frequency sketch). Single-threaded command execution per the spec's C1 constraint.

**Tech Stack:** Kotlin 2.2.x, JDK 21, Maven 3.9.8, JUnit 5, AssertJ, Netty 4.1.x, LuaJ 3.0.x

**Plan conventions:**
- `$MVN` = `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`
- `$ROOT` = `/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/DynaCache`
- Every sub-phase ends with all prior tests green + a commit.

**Pre-reading for P1:**
- Skip Lists (Pugh, 1990) — 8 pages
- Hashed and Hierarchical Timing Wheels (Varghese & Lauck, 1987) — 14 pages
- TinyLFU (Einziger et al., 2017) — 15 pages
- Caffeine source: `FrequencySketch.java` — ~200 LOC
- Redis RESP2 protocol spec — ~5 pages
- Redis source: `t_zset.c` (skip list + hash dual index) — ~500 LOC
- Redis source: `dict.c` — `dictScan()` (~80 LOC, reverse binary iteration) + `_dictRehashStep()` (~50 LOC, incremental rehashing)

---

## Sub-phase 1A: Project Scaffold + Core Types

**Concept:** Maven multi-module layout. Establish the engine module with zero I/O deps.

### Task 1: Maven project skeleton

**Files:**
- Create: `$ROOT/pom.xml` (parent POM)
- Create: `$ROOT/dynacache-engine/pom.xml`
- Create: `$ROOT/dynacache-server/pom.xml`
- Create: `$ROOT/.gitignore`
- Create: `$ROOT/README.md`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p "$ROOT"
cd "$ROOT"
git init
mkdir -p dynacache-engine/src/main/kotlin/dynacache/engine
mkdir -p dynacache-engine/src/test/kotlin/dynacache/engine
mkdir -p dynacache-server/src/main/kotlin/dynacache/server
mkdir -p dynacache-server/src/test/kotlin/dynacache/server
```

- [ ] **Step 2: Write parent pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <groupId>dynacache</groupId>
    <artifactId>dynacache-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>DynaCache (parent)</name>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <kotlin.version>2.2.0</kotlin.version>
        <kotlin.code.style>official</kotlin.code.style>
        <junit.version>5.11.0</junit.version>
        <assertj.version>3.26.3</assertj.version>
        <netty.version>4.1.108.Final</netty.version>
        <luaj.version>3.0.1</luaj.version>
        <grpc-kotlin.version>1.4.1</grpc-kotlin.version>
        <grpc.version>1.63.0</grpc.version>
        <protobuf.version>3.25.3</protobuf.version>
        <coroutines.version>1.10.1</coroutines.version>
    </properties>

    <modules>
        <module>dynacache-engine</module>
        <module>dynacache-server</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-stdlib</artifactId>
                <version>${kotlin.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jetbrains.kotlinx</groupId>
                <artifactId>kotlinx-coroutines-core</artifactId>
                <version>${coroutines.version}</version>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <sourceDirectory>src/main/kotlin</sourceDirectory>
        <testSourceDirectory>src/test/kotlin</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-maven-plugin</artifactId>
                <version>${kotlin.version}</version>
                <executions>
                    <execution>
                        <id>compile</id>
                        <goals><goal>compile</goal></goals>
                    </execution>
                    <execution>
                        <id>test-compile</id>
                        <goals><goal>test-compile</goal></goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Write dynacache-engine/pom.xml**

Engine depends ONLY on kotlin-stdlib (compile-time enforcement of pure-engine invariant).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>dynacache</groupId>
        <artifactId>dynacache-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>dynacache-engine</artifactId>
    <name>DynaCache Engine (pure, no I/O)</name>

    <dependencies>
        <dependency>
            <groupId>org.jetbrains.kotlin</groupId>
            <artifactId>kotlin-stdlib</artifactId>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Write dynacache-server/pom.xml**

Server depends on engine + Netty + LuaJ + coroutines.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>dynacache</groupId>
        <artifactId>dynacache-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>dynacache-server</artifactId>
    <name>DynaCache Server (Netty RESP + gRPC)</name>

    <dependencies>
        <dependency>
            <groupId>dynacache</groupId>
            <artifactId>dynacache-engine</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.jetbrains.kotlinx</groupId>
            <artifactId>kotlinx-coroutines-core</artifactId>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-all</artifactId>
            <version>${netty.version}</version>
        </dependency>
        <dependency>
            <groupId>org.luaj</groupId>
            <artifactId>luaj-jse</artifactId>
            <version>${luaj.version}</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 5: Write .gitignore and README.md**

`.gitignore`:
```
target/
*.iml
.idea/
*.class
```

`README.md`:
```markdown
# DynaCache

Dynamo-style AP distributed cache with Redis-compatible data structures and RESP2 wire protocol.

Built from scratch in Kotlin as a learning project.
```

- [ ] **Step 6: Verify build**

```bash
cd "$ROOT" && $MVN package -q
```

Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: Maven multi-module scaffold — engine (pure) + server (Netty)"
```

### Task 2: Core command model and key-value store interface

**Concept:** Define the command/response types that flow through the engine. The engine has one entry point: `execute(command): Response`. Every data structure command is a sealed class variant.

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/Command.kt`
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/Response.kt`
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/DataType.kt`
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/KeyEntry.kt`
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/DataEngineTest.kt`

- [ ] **Step 1: Write Command.kt — sealed hierarchy for all commands**

```kotlin
package dynacache.engine

/** Every Redis command maps to a sealed variant. */
sealed class Command {
    abstract val key: String?  // null for server commands (PING, DBSIZE, etc.)

    // ── String ──
    data class Get(override val key: String) : Command()
    data class Set(
        override val key: String,
        val value: ByteArray,
        val nx: Boolean = false,
        val xx: Boolean = false,
        val exSeconds: Long? = null,
        val pxMillis: Long? = null,
    ) : Command()
    data class Incr(override val key: String) : Command()
    data class Decr(override val key: String) : Command()
    data class IncrBy(override val key: String, val delta: Long) : Command()
    data class DecrBy(override val key: String, val delta: Long) : Command()
    data class Append(override val key: String, val value: ByteArray) : Command()
    data class Strlen(override val key: String) : Command()
    data class MGet(val keys: List<String>) : Command() { override val key: String? = null }
    data class MSet(val entries: List<Pair<String, ByteArray>>) : Command() { override val key: String? = null }

    // ── Hash ──
    data class HGet(override val key: String, val field: String) : Command()
    data class HSet(override val key: String, val fields: Map<String, ByteArray>) : Command()
    data class HDel(override val key: String, val fields: List<String>) : Command()
    data class HGetAll(override val key: String) : Command()
    data class HMGet(override val key: String, val fields: List<String>) : Command()
    data class HExists(override val key: String, val field: String) : Command()
    data class HKeys(override val key: String) : Command()
    data class HVals(override val key: String) : Command()
    data class HLen(override val key: String) : Command()

    // ── List ──
    data class LPush(override val key: String, val values: List<ByteArray>) : Command()
    data class RPush(override val key: String, val values: List<ByteArray>) : Command()
    data class LPop(override val key: String) : Command()
    data class RPop(override val key: String) : Command()
    data class LRange(override val key: String, val start: Long, val stop: Long) : Command()
    data class LLen(override val key: String) : Command()
    data class LIndex(override val key: String, val index: Long) : Command()
    data class LSet(override val key: String, val index: Long, val value: ByteArray) : Command()
    data class LRem(override val key: String, val count: Long, val value: ByteArray) : Command()

    // ── Sorted Set ──
    data class ZAdd(override val key: String, val members: List<Pair<Double, String>>) : Command()
    data class ZRem(override val key: String, val members: List<String>) : Command()
    data class ZRange(override val key: String, val start: Long, val stop: Long, val withScores: Boolean = false) : Command()
    data class ZRevRange(override val key: String, val start: Long, val stop: Long, val withScores: Boolean = false) : Command()
    data class ZRangeByScore(override val key: String, val min: Double, val max: Double, val withScores: Boolean = false) : Command()
    data class ZRank(override val key: String, val member: String) : Command()
    data class ZRevRank(override val key: String, val member: String) : Command()
    data class ZScore(override val key: String, val member: String) : Command()
    data class ZCard(override val key: String) : Command()
    data class ZIncrBy(override val key: String, val increment: Double, val member: String) : Command()

    // ── Key / TTL ──
    data class Del(val keys: List<String>) : Command() { override val key: String? = null }
    data class Exists(val keys: List<String>) : Command() { override val key: String? = null }
    data class Type(override val key: String) : Command()
    data class Expire(override val key: String, val seconds: Long) : Command()
    data class PExpire(override val key: String, val millis: Long) : Command()
    data class ExpireAt(override val key: String, val timestampSeconds: Long) : Command()
    data class Ttl(override val key: String) : Command()
    data class PTtl(override val key: String) : Command()
    data class Persist(override val key: String) : Command()
    data class Keys(val pattern: String) : Command() { override val key: String? = null }
    data class RandomKey(val dummy: Unit = Unit) : Command() { override val key: String? = null }

    // ── Server ──
    data class Ping(val message: String? = null) : Command() { override val key: String? = null }
    data class DbSize(val dummy: Unit = Unit) : Command() { override val key: String? = null }
    data class FlushDb(val dummy: Unit = Unit) : Command() { override val key: String? = null }
    data class Info(val section: String? = null) : Command() { override val key: String? = null }
    data class CommandCmd(val dummy: Unit = Unit) : Command() { override val key: String? = null }
}
```

- [ ] **Step 2: Write Response.kt**

```kotlin
package dynacache.engine

/** RESP-compatible response types. */
sealed class Response {
    data class SimpleString(val value: String) : Response()            // +OK
    data class Error(val type: String, val message: String) : Response() // -ERR ...
    data class IntegerReply(val value: Long) : Response()              // :123
    data class BulkString(val value: ByteArray?) : Response()          // $N\r\n...
    data class ArrayReply(val values: List<Response>?) : Response()    // *N\r\n...

    companion object {
        val OK = SimpleString("OK")
        val NIL = BulkString(null)
        val EMPTY_ARRAY = ArrayReply(emptyList())
        fun wrongType() = Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        fun error(msg: String) = Error("ERR", msg)
        fun integer(v: Long) = IntegerReply(v)
        fun bulk(v: ByteArray) = BulkString(v)
        fun bulk(v: String) = BulkString(v.toByteArray())
    }
}
```

- [ ] **Step 3: Write DataType.kt and KeyEntry.kt**

```kotlin
// DataType.kt
package dynacache.engine

enum class DataType { STRING, HASH, LIST, ZSET }
```

```kotlin
// KeyEntry.kt
package dynacache.engine

/**
 * Every key in the store wraps its typed value + metadata.
 * expiresAtMs: absolute epoch millis, or -1 for no expiry.
 */
data class KeyEntry(
    val type: DataType,
    val value: Any,       // ByteArray | HashMap<String,ByteArray> | ArrayDeque<ByteArray> | SortedSetValue
    var expiresAtMs: Long = -1L,
    var lastAccessMs: Long = 0L,
)
```

- [ ] **Step 4: Write DataEngine.kt — stub with PING + GET + SET**

```kotlin
package dynacache.engine

class DataEngine(
    private val clock: () -> Long = System::currentTimeMillis
) {
    private val store = HashMap<String, KeyEntry>()

    fun execute(cmd: Command): Response = when (cmd) {
        is Command.Ping -> if (cmd.message != null) Response.bulk(cmd.message) else Response.SimpleString("PONG")
        is Command.Set -> handleSet(cmd)
        is Command.Get -> handleGet(cmd)
        else -> Response.error("ERR unknown command")
    }

    private fun handleSet(cmd: Command.Set): Response {
        val existing = getIfAlive(cmd.key)
        if (cmd.nx && existing != null) return Response.NIL
        if (cmd.xx && existing == null) return Response.NIL
        if (existing != null && existing.type != DataType.STRING) return Response.wrongType()

        val entry = KeyEntry(
            type = DataType.STRING,
            value = cmd.value,
            lastAccessMs = clock(),
        )
        when {
            cmd.exSeconds != null -> entry.expiresAtMs = clock() + cmd.exSeconds * 1000
            cmd.pxMillis != null -> entry.expiresAtMs = clock() + cmd.pxMillis
        }
        store[cmd.key] = entry
        return Response.OK
    }

    private fun handleGet(cmd: Command.Get): Response {
        val entry = getIfAlive(cmd.key) ?: return Response.NIL
        if (entry.type != DataType.STRING) return Response.wrongType()
        entry.lastAccessMs = clock()
        return Response.BulkString(entry.value as ByteArray)
    }

    /** Lazy expiry check: if key exists but is expired, remove it and return null. */
    private fun getIfAlive(key: String): KeyEntry? {
        val entry = store[key] ?: return null
        if (entry.expiresAtMs != -1L && clock() >= entry.expiresAtMs) {
            store.remove(key)
            return null
        }
        return entry
    }

    fun keyCount(): Int = store.size
}
```

- [ ] **Step 5: Write test — string_set_get_roundtrip + string_set_nx + PING**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class DataEngineTest {
    private val engine = DataEngine()

    @Test
    fun `PING returns PONG`() {
        val res = engine.execute(Command.Ping())
        assertThat(res).isEqualTo(Response.SimpleString("PONG"))
    }

    @Test
    fun `SET then GET roundtrip`() {
        engine.execute(Command.Set("foo", "bar".toByteArray()))
        val res = engine.execute(Command.Get("foo"))
        assertThat((res as Response.BulkString).value).isEqualTo("bar".toByteArray())
    }

    @Test
    fun `GET missing key returns NIL`() {
        val res = engine.execute(Command.Get("missing"))
        assertThat((res as Response.BulkString).value).isNull()
    }

    @Test
    fun `SET NX rejects existing key`() {
        engine.execute(Command.Set("k", "v1".toByteArray()))
        val res = engine.execute(Command.Set("k", "v2".toByteArray(), nx = true))
        assertThat((res as Response.BulkString).value).isNull()
        // original value unchanged
        val get = engine.execute(Command.Get("k")) as Response.BulkString
        assertThat(get.value).isEqualTo("v1".toByteArray())
    }

    @Test
    fun `SET XX rejects missing key`() {
        val res = engine.execute(Command.Set("k", "v".toByteArray(), xx = true))
        assertThat((res as Response.BulkString).value).isNull()
    }

    @Test
    fun `SET EX expires key`() {
        var now = 1000L
        val eng = DataEngine(clock = { now })
        eng.execute(Command.Set("k", "v".toByteArray(), exSeconds = 2))

        // before expiry
        now = 2999L
        assertThat((eng.execute(Command.Get("k")) as Response.BulkString).value).isNotNull()

        // after expiry
        now = 3000L
        assertThat((eng.execute(Command.Get("k")) as Response.BulkString).value).isNull()
    }
}
```

- [ ] **Step 6: Run tests**

```bash
cd "$ROOT" && $MVN test -pl dynacache-engine -q
```

Expected: 5 tests pass.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(engine): core types (Command, Response, KeyEntry) + String GET/SET with NX/XX/EX"
```

---

## Sub-phase 1B: String Completion + Hash + List

**Concept:** Complete all String commands (INCR/DECR/APPEND/STRLEN/MGET/MSET), then Hash and List data structures. Each is a self-contained key type with its own storage. Learn type-checking discipline (WRONGTYPE errors).

### Task 3: String arithmetic — INCR, DECR, INCRBY, DECRBY

**Files:**
- Modify: `dynacache-engine/src/main/kotlin/dynacache/engine/DataEngine.kt`
- Modify: `dynacache-engine/src/test/kotlin/dynacache/engine/DataEngineTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
@Test
fun `INCR on missing key returns 1`() {
    val res = engine.execute(Command.Incr("counter"))
    assertThat(res).isEqualTo(Response.IntegerReply(1))
    val get = engine.execute(Command.Get("counter")) as Response.BulkString
    assertThat(String(get.value!!)).isEqualTo("1")
}

@Test
fun `INCR on numeric string increments`() {
    engine.execute(Command.Set("c", "10".toByteArray()))
    val res = engine.execute(Command.Incr("c"))
    assertThat(res).isEqualTo(Response.IntegerReply(11))
}

@Test
fun `INCR on non-numeric string returns error`() {
    engine.execute(Command.Set("c", "notanumber".toByteArray()))
    val res = engine.execute(Command.Incr("c"))
    assertThat(res).isInstanceOf(Response.Error::class.java)
}

@Test
fun `DECRBY subtracts correctly`() {
    engine.execute(Command.Set("c", "100".toByteArray()))
    val res = engine.execute(Command.DecrBy("c", 30))
    assertThat(res).isEqualTo(Response.IntegerReply(70))
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
cd "$ROOT" && $MVN test -pl dynacache-engine -q
```

Expected: FAIL — unhandled command types.

- [ ] **Step 3: Implement INCR/DECR/INCRBY/DECRBY in DataEngine**

Add to the `when` block in `execute()` and implement `handleIncr(key, delta)`:

```kotlin
is Command.Incr -> handleIncr(cmd.key, 1)
is Command.Decr -> handleIncr(cmd.key, -1)
is Command.IncrBy -> handleIncr(cmd.key, cmd.delta)
is Command.DecrBy -> handleIncr(cmd.key, -cmd.delta)
```

```kotlin
private fun handleIncr(key: String, delta: Long): Response {
    val entry = getIfAlive(key)
    if (entry != null && entry.type != DataType.STRING) return Response.wrongType()

    val current = if (entry == null) 0L else {
        val str = String(entry.value as ByteArray)
        str.toLongOrNull() ?: return Response.error("ERR value is not an integer or out of range")
    }
    val newVal = current + delta
    val newEntry = KeyEntry(
        type = DataType.STRING,
        value = newVal.toString().toByteArray(),
        expiresAtMs = entry?.expiresAtMs ?: -1L,
        lastAccessMs = clock(),
    )
    store[key] = newEntry
    return Response.IntegerReply(newVal)
}
```

- [ ] **Step 4: Run tests — verify they pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): INCR/DECR/INCRBY/DECRBY with type checking"
```

### Task 4: APPEND, STRLEN, MGET, MSET

**Files:**
- Modify: `DataEngine.kt`
- Modify: `DataEngineTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
@Test
fun `APPEND to existing key concatenates`() {
    engine.execute(Command.Set("k", "hello".toByteArray()))
    val res = engine.execute(Command.Append("k", " world".toByteArray()))
    assertThat(res).isEqualTo(Response.IntegerReply(11)) // new length
    val get = engine.execute(Command.Get("k")) as Response.BulkString
    assertThat(String(get.value!!)).isEqualTo("hello world")
}

@Test
fun `STRLEN returns byte length`() {
    engine.execute(Command.Set("k", "hello".toByteArray()))
    assertThat(engine.execute(Command.Strlen("k"))).isEqualTo(Response.IntegerReply(5))
}

@Test
fun `MGET returns values in order, NIL for missing`() {
    engine.execute(Command.Set("a", "1".toByteArray()))
    engine.execute(Command.Set("c", "3".toByteArray()))
    val res = engine.execute(Command.MGet(listOf("a", "b", "c"))) as Response.ArrayReply
    assertThat(res.values).hasSize(3)
    assertThat((res.values!![0] as Response.BulkString).value).isEqualTo("1".toByteArray())
    assertThat((res.values!![1] as Response.BulkString).value).isNull() // b missing
    assertThat((res.values!![2] as Response.BulkString).value).isEqualTo("3".toByteArray())
}

@Test
fun `MSET sets multiple keys atomically`() {
    engine.execute(Command.MSet(listOf("x" to "1".toByteArray(), "y" to "2".toByteArray())))
    assertThat((engine.execute(Command.Get("x")) as Response.BulkString).value).isEqualTo("1".toByteArray())
    assertThat((engine.execute(Command.Get("y")) as Response.BulkString).value).isEqualTo("2".toByteArray())
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement APPEND, STRLEN, MGET, MSET**

```kotlin
is Command.Append -> handleAppend(cmd)
is Command.Strlen -> handleStrlen(cmd)
is Command.MGet -> handleMGet(cmd)
is Command.MSet -> handleMSet(cmd)
```

```kotlin
private fun handleAppend(cmd: Command.Append): Response {
    val entry = getIfAlive(cmd.key)
    if (entry != null && entry.type != DataType.STRING) return Response.wrongType()
    val existing = if (entry != null) entry.value as ByteArray else ByteArray(0)
    val newVal = existing + cmd.value
    store[cmd.key] = KeyEntry(DataType.STRING, newVal, entry?.expiresAtMs ?: -1L, clock())
    return Response.IntegerReply(newVal.size.toLong())
}

private fun handleStrlen(cmd: Command.Strlen): Response {
    val entry = getIfAlive(cmd.key) ?: return Response.IntegerReply(0)
    if (entry.type != DataType.STRING) return Response.wrongType()
    return Response.IntegerReply((entry.value as ByteArray).size.toLong())
}

private fun handleMGet(cmd: Command.MGet): Response {
    val results = cmd.keys.map { key ->
        val entry = getIfAlive(key)
        if (entry == null || entry.type != DataType.STRING) Response.NIL
        else Response.BulkString(entry.value as ByteArray)
    }
    return Response.ArrayReply(results)
}

private fun handleMSet(cmd: Command.MSet): Response {
    for ((key, value) in cmd.entries) {
        store[key] = KeyEntry(DataType.STRING, value, lastAccessMs = clock())
    }
    return Response.OK
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): APPEND/STRLEN/MGET/MSET — String commands complete"
```

### Task 5: Hash commands

**Files:**
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/HashCommandTest.kt`

- [ ] **Step 1: Write failing tests in HashCommandTest.kt**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HashCommandTest {
    private val engine = DataEngine()

    @Test
    fun `HSET and HGET roundtrip`() {
        engine.execute(Command.HSet("h", mapOf("f1" to "v1".toByteArray())))
        val res = engine.execute(Command.HGet("h", "f1"))
        assertThat((res as Response.BulkString).value).isEqualTo("v1".toByteArray())
    }

    @Test
    fun `HGET missing field returns NIL`() {
        engine.execute(Command.HSet("h", mapOf("f1" to "v1".toByteArray())))
        assertThat((engine.execute(Command.HGet("h", "nope")) as Response.BulkString).value).isNull()
    }

    @Test
    fun `HDEL removes field, others unaffected`() {
        engine.execute(Command.HSet("h", mapOf("a" to "1".toByteArray(), "b" to "2".toByteArray())))
        engine.execute(Command.HDel("h", listOf("a")))
        assertThat((engine.execute(Command.HGet("h", "a")) as Response.BulkString).value).isNull()
        assertThat((engine.execute(Command.HGet("h", "b")) as Response.BulkString).value).isEqualTo("2".toByteArray())
    }

    @Test
    fun `HGETALL returns all fields`() {
        engine.execute(Command.HSet("h", mapOf("a" to "1".toByteArray(), "b" to "2".toByteArray())))
        val res = engine.execute(Command.HGetAll("h")) as Response.ArrayReply
        // Redis HGETALL returns [field, value, field, value, ...]
        assertThat(res.values).hasSize(4)
    }

    @Test
    fun `HLEN returns field count`() {
        engine.execute(Command.HSet("h", mapOf("a" to "1".toByteArray(), "b" to "2".toByteArray())))
        assertThat(engine.execute(Command.HLen("h"))).isEqualTo(Response.IntegerReply(2))
    }

    @Test
    fun `HEXISTS returns 1 for present, 0 for absent`() {
        engine.execute(Command.HSet("h", mapOf("a" to "1".toByteArray())))
        assertThat(engine.execute(Command.HExists("h", "a"))).isEqualTo(Response.IntegerReply(1))
        assertThat(engine.execute(Command.HExists("h", "nope"))).isEqualTo(Response.IntegerReply(0))
    }

    @Test
    fun `WRONGTYPE when HGET on string key`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        val res = engine.execute(Command.HGet("k", "f"))
        assertThat(res).isInstanceOf(Response.Error::class.java)
        assertThat((res as Response.Error).type).isEqualTo("WRONGTYPE")
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement Hash commands in DataEngine**

Add Hash command routing and implement using `HashMap<String, ByteArray>` as the value type. Each hash command must:
1. Check if key exists and is expired (lazy expiry)
2. Check type is HASH (or key doesn't exist)
3. Create the HashMap on first write
4. Perform the operation

```kotlin
is Command.HGet -> handleHGet(cmd)
is Command.HSet -> handleHSet(cmd)
is Command.HDel -> handleHDel(cmd)
is Command.HGetAll -> handleHGetAll(cmd)
is Command.HMGet -> handleHMGet(cmd)
is Command.HExists -> handleHExists(cmd)
is Command.HKeys -> handleHKeys(cmd)
is Command.HVals -> handleHVals(cmd)
is Command.HLen -> handleHLen(cmd)
```

Implementation pattern (example for HSET):
```kotlin
private fun handleHSet(cmd: Command.HSet): Response {
    val entry = getIfAlive(cmd.key)
    if (entry != null && entry.type != DataType.HASH) return Response.wrongType()

    @Suppress("UNCHECKED_CAST")
    val map = if (entry != null) entry.value as HashMap<String, ByteArray>
              else HashMap<String, ByteArray>()
    var added = 0L
    for ((field, value) in cmd.fields) {
        if (!map.containsKey(field)) added++
        map[field] = value
    }
    if (entry == null) {
        store[cmd.key] = KeyEntry(DataType.HASH, map, lastAccessMs = clock())
    } else {
        entry.lastAccessMs = clock()
    }
    return Response.IntegerReply(added)
}
```

Follow the same pattern for each Hash command. HGETALL returns alternating field/value as BulkString entries in an ArrayReply. HKEYS/HVALS return arrays. HEXISTS returns integer 0 or 1.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): Hash commands — HSET/HGET/HDEL/HGETALL/HMGET/HEXISTS/HKEYS/HVALS/HLEN"
```

### Task 6: List commands

**Files:**
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/ListCommandTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ListCommandTest {
    private val engine = DataEngine()

    @Test
    fun `LPUSH then RPOP returns FIFO order`() {
        // LPUSH pushes left: LPUSH k a b c → list is [c, b, a]
        engine.execute(Command.LPush("k", listOf("a", "b", "c").map { it.toByteArray() }))
        // RPOP returns rightmost = "a"
        val res = engine.execute(Command.RPop("k")) as Response.BulkString
        assertThat(String(res.value!!)).isEqualTo("a")
    }

    @Test
    fun `RPUSH then LPOP returns FIFO order`() {
        engine.execute(Command.RPush("k", listOf("a", "b", "c").map { it.toByteArray() }))
        val res = engine.execute(Command.LPop("k")) as Response.BulkString
        assertThat(String(res.value!!)).isEqualTo("a")
    }

    @Test
    fun `LRANGE with out-of-bounds clamps`() {
        engine.execute(Command.RPush("k", listOf("a", "b", "c").map { it.toByteArray() }))
        val res = engine.execute(Command.LRange("k", -100, 100)) as Response.ArrayReply
        assertThat(res.values).hasSize(3)
    }

    @Test
    fun `LRANGE with negative indices`() {
        engine.execute(Command.RPush("k", listOf("a", "b", "c").map { it.toByteArray() }))
        // LRANGE k -2 -1 → last two elements: ["b", "c"]
        val res = engine.execute(Command.LRange("k", -2, -1)) as Response.ArrayReply
        assertThat(res.values).hasSize(2)
        assertThat(String((res.values!![0] as Response.BulkString).value!!)).isEqualTo("b")
    }

    @Test
    fun `LLEN returns length`() {
        engine.execute(Command.RPush("k", listOf("a", "b").map { it.toByteArray() }))
        assertThat(engine.execute(Command.LLen("k"))).isEqualTo(Response.IntegerReply(2))
    }

    @Test
    fun `POP on empty key returns NIL`() {
        assertThat((engine.execute(Command.LPop("k")) as Response.BulkString).value).isNull()
    }

    @Test
    fun `WRONGTYPE when LPUSH on string key`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        val res = engine.execute(Command.LPush("k", listOf("x".toByteArray())))
        assertThat(res).isInstanceOf(Response.Error::class.java)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement List commands using ArrayDeque**

```kotlin
is Command.LPush -> handleLPush(cmd)
is Command.RPush -> handleRPush(cmd)
is Command.LPop -> handleLPop(cmd)
is Command.RPop -> handleRPop(cmd)
is Command.LRange -> handleLRange(cmd)
is Command.LLen -> handleLLen(cmd)
is Command.LIndex -> handleLIndex(cmd)
is Command.LSet -> handleLSet(cmd)
is Command.LRem -> handleLRem(cmd)
```

Internal storage is `ArrayDeque<ByteArray>`. LPUSH adds to front, RPUSH adds to back. LRANGE resolves negative indices per Redis convention: -1 = last element, -2 = second-to-last. Clamp to [0, size-1].

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): List commands — LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET/LREM"
```

### Task 7: Key management — DEL, EXISTS, TYPE, DBSIZE, FLUSHDB, KEYS

**Files:**
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/KeyCommandTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class KeyCommandTest {
    private val engine = DataEngine()

    @Test
    fun `DEL removes key, returns count`() {
        engine.execute(Command.Set("a", "1".toByteArray()))
        engine.execute(Command.Set("b", "2".toByteArray()))
        val res = engine.execute(Command.Del(listOf("a", "b", "nonexistent")))
        assertThat(res).isEqualTo(Response.IntegerReply(2))
        assertThat((engine.execute(Command.Get("a")) as Response.BulkString).value).isNull()
    }

    @Test
    fun `EXISTS counts existing keys`() {
        engine.execute(Command.Set("a", "1".toByteArray()))
        assertThat(engine.execute(Command.Exists(listOf("a", "missing")))).isEqualTo(Response.IntegerReply(1))
    }

    @Test
    fun `TYPE returns correct type name`() {
        engine.execute(Command.Set("s", "v".toByteArray()))
        engine.execute(Command.HSet("h", mapOf("f" to "v".toByteArray())))
        engine.execute(Command.RPush("l", listOf("v".toByteArray())))
        assertThat(engine.execute(Command.Type("s"))).isEqualTo(Response.SimpleString("string"))
        assertThat(engine.execute(Command.Type("h"))).isEqualTo(Response.SimpleString("hash"))
        assertThat(engine.execute(Command.Type("l"))).isEqualTo(Response.SimpleString("list"))
        assertThat(engine.execute(Command.Type("missing"))).isEqualTo(Response.SimpleString("none"))
    }

    @Test
    fun `DBSIZE returns key count`() {
        engine.execute(Command.Set("a", "1".toByteArray()))
        engine.execute(Command.Set("b", "2".toByteArray()))
        assertThat(engine.execute(Command.DbSize())).isEqualTo(Response.IntegerReply(2))
    }

    @Test
    fun `FLUSHDB clears all keys`() {
        engine.execute(Command.Set("a", "1".toByteArray()))
        engine.execute(Command.FlushDb())
        assertThat(engine.execute(Command.DbSize())).isEqualTo(Response.IntegerReply(0))
    }

    @Test
    fun `KEYS with glob pattern`() {
        engine.execute(Command.Set("user:1", "a".toByteArray()))
        engine.execute(Command.Set("user:2", "b".toByteArray()))
        engine.execute(Command.Set("order:1", "c".toByteArray()))
        val res = engine.execute(Command.Keys("user:*")) as Response.ArrayReply
        assertThat(res.values).hasSize(2)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement key management commands**

DEL/EXISTS iterate the key list. TYPE maps `DataType` to the Redis type name string (`"string"`, `"hash"`, `"list"`, `"zset"`, `"none"`). KEYS implements glob matching — support `*` (any chars), `?` (one char), `[abc]` (char class) by converting the Redis glob to a regex.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): key management — DEL/EXISTS/TYPE/DBSIZE/FLUSHDB/KEYS"
```

### Task 7B: SCAN — Cursor-based iteration with reverse binary iteration

**Concept:** `KEYS *` blocks the engine and is O(n) — unusable with large datasets. SCAN solves this with cursor-based iteration: stateless, non-blocking, handles rehashing mid-scan. The key insight is **reverse binary iteration** — iterating hash table buckets in bit-reversed order so that a hash table resize mid-scan never causes missed keys.

This requires building a custom hash table (not `java.util.HashMap`) with:
- Open addressing or chaining with known bucket layout
- Incremental rehashing — migrate one bucket per operation instead of stop-the-world resize
- Reverse binary cursor — the core SCAN algorithm

**Learning goals:**
- Hash table internals: bucket layout, load factor, resize triggers
- Incremental rehashing (same idea as LSM compaction — amortize expensive restructuring)
- Cursor design patterns (appears in every database: SQL cursors, DynamoDB LastEvaluatedKey, Kafka offsets, paginated APIs)
- Consistency-completeness tradeoff: SCAN may duplicate, never miss

**Pre-reading:**
- Redis source: `dict.c` — `dictScan()` function (~80 LOC, the reverse binary iteration algorithm)
- Redis source: `dict.c` — `_dictRehashStep()` (incremental rehashing)
- Antirez blog post on SCAN (explains the cursor math)

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/ScanHashTable.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/ScanHashTableTest.kt`
- Modify: `dynacache-engine/src/main/kotlin/dynacache/engine/DataEngine.kt`
- Modify: `dynacache-engine/src/test/kotlin/dynacache/engine/KeyCommandTest.kt`

- [ ] **Step 1: Write failing tests for the custom hash table**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ScanHashTableTest {
    @Test
    fun `put and get roundtrip`() {
        val ht = ScanHashTable<String, String>()
        ht.put("a", "1")
        assertThat(ht.get("a")).isEqualTo("1")
    }

    @Test
    fun `remove returns old value`() {
        val ht = ScanHashTable<String, String>()
        ht.put("a", "1")
        assertThat(ht.remove("a")).isEqualTo("1")
        assertThat(ht.get("a")).isNull()
    }

    @Test
    fun `incremental rehash spreads across operations`() {
        val ht = ScanHashTable<String, String>(initialCapacity = 4)
        // Insert enough to trigger rehash
        for (i in 0 until 20) ht.put("key$i", "val$i")
        // All keys retrievable after rehash
        for (i in 0 until 20) assertThat(ht.get("key$i")).isEqualTo("val$i")
    }

    @Test
    fun `scan returns all keys across full iteration`() {
        val ht = ScanHashTable<String, String>()
        val expected = (0 until 100).map { "key$it" }.toSet()
        expected.forEach { ht.put(it, "v") }

        val found = mutableSetOf<String>()
        var cursor = 0L
        do {
            val result = ht.scan(cursor, count = 10)
            found.addAll(result.entries.map { it.key })
            cursor = result.cursor
        } while (cursor != 0L)

        assertThat(found).containsAll(expected)  // never misses
    }

    @Test
    fun `scan during rehash misses no keys`() {
        val ht = ScanHashTable<String, String>(initialCapacity = 4)
        val keys = (0 until 50).map { "key$it" }.toSet()
        keys.forEach { ht.put(it, "v") }

        val found = mutableSetOf<String>()
        var cursor = 0L
        var steps = 0
        do {
            val result = ht.scan(cursor, count = 5)
            found.addAll(result.entries.map { it.key })
            cursor = result.cursor
            // Insert more keys mid-scan to trigger rehash
            if (steps < 10) ht.put("extra$steps", "v")
            steps++
        } while (cursor != 0L)

        // All original keys must be found (may have duplicates — that's OK)
        assertThat(found).containsAll(keys)
    }

    @Test
    fun `scan with pattern filtering`() {
        val ht = ScanHashTable<String, String>()
        ht.put("user:1", "a")
        ht.put("user:2", "b")
        ht.put("order:1", "c")

        val found = mutableSetOf<String>()
        var cursor = 0L
        do {
            val result = ht.scan(cursor, count = 10, pattern = "user:*")
            found.addAll(result.entries.map { it.key })
            cursor = result.cursor
        } while (cursor != 0L)

        assertThat(found).containsExactlyInAnyOrder("user:1", "user:2")
    }
}
```

- [ ] **Step 2: Run tests — verify they fail**

- [ ] **Step 3: Implement ScanHashTable**

Core components:

1. **Bucket array** with chaining (linked list per bucket). Power-of-2 sizing.

2. **Incremental rehashing**: when load factor > 0.75, allocate `ht[1]` at 2x size. Each `put`/`get`/`remove`/`scan` call migrates one bucket from `ht[0]` to `ht[1]`. When `ht[0]` is empty, swap and null out `ht[1]`.

3. **Reverse binary iteration** for `scan()`:

```kotlin
data class ScanResult<K, V>(
    val cursor: Long,
    val entries: List<Map.Entry<K, V>>,
)

fun scan(cursor: Long, count: Int = 10, pattern: String? = null): ScanResult<K, V> {
    val results = mutableListOf<Map.Entry<K, V>>()
    var c = cursor

    // If rehashing, scan both tables
    val scanned = if (isRehashing) {
        scanBothTables(c, count, results)
    } else {
        scanSingleTable(c, count, results)
    }

    // Pattern filter
    val filtered = if (pattern != null) {
        val regex = globToRegex(pattern)
        results.filter { regex.matches(it.key.toString()) }
    } else results

    return ScanResult(scanned, filtered)
}

// The reverse binary iteration: reverse bits, increment, reverse back
private fun nextCursor(cursor: Long, mask: Long): Long {
    var v = cursor or mask.inv()   // set high bits
    v = v.reverseBits()            // reverse
    v++                            // increment
    v = v.reverseBits()            // reverse back
    return v and mask              // mask to table size
}
```

- [ ] **Step 4: Run tests — verify pass**

- [ ] **Step 5: Integrate into DataEngine — replace HashMap with ScanHashTable**

Replace the internal `store: HashMap<String, KeyEntry>` with `ScanHashTable<String, KeyEntry>`. Add `Command.Scan` and wire it:

```kotlin
is Command.Scan -> {
    val result = store.scan(cmd.cursor, cmd.count, cmd.pattern)
    Response.ArrayReply(listOf(
        Response.BulkString(result.cursor.toString().toByteArray()),
        Response.ArrayReply(result.entries.map {
            Response.BulkString(it.key.toByteArray())
        })
    ))
}
```

Also add HSCAN, SSCAN, ZSCAN as variants that scan within a single key's data structure.

- [ ] **Step 6: Write integration tests for SCAN command**

```kotlin
@Test
fun `SCAN iterates all keys`() {
    for (i in 0 until 100) engine.execute(Command.Set("key$i", "v".toByteArray()))

    val found = mutableSetOf<String>()
    var cursor = 0L
    do {
        val res = engine.execute(Command.Scan(cursor, count = 10)) as Response.ArrayReply
        cursor = String((res.values[0] as Response.BulkString).value!!).toLong()
        val keys = (res.values[1] as Response.ArrayReply).values
            .map { String((it as Response.BulkString).value!!) }
        found.addAll(keys)
    } while (cursor != 0L)

    assertThat(found).hasSize(100)
}

@Test
fun `SCAN with MATCH pattern`() {
    engine.execute(Command.Set("user:1", "a".toByteArray()))
    engine.execute(Command.Set("user:2", "b".toByteArray()))
    engine.execute(Command.Set("order:1", "c".toByteArray()))

    val found = mutableSetOf<String>()
    var cursor = 0L
    do {
        val res = engine.execute(Command.Scan(cursor, count = 100, pattern = "user:*")) as Response.ArrayReply
        cursor = String((res.values[0] as Response.BulkString).value!!).toLong()
        val keys = (res.values[1] as Response.ArrayReply).values
            .map { String((it as Response.BulkString).value!!) }
        found.addAll(keys)
    } while (cursor != 0L)

    assertThat(found).containsExactlyInAnyOrder("user:1", "user:2")
}
```

- [ ] **Step 7: Run all tests — verify pass**
- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(engine): SCAN with reverse binary iteration + custom hash table with incremental rehashing"
```

---

## Sub-phase 1C: Skip List + Sorted Set

**Concept:** Build a skip list from scratch — the most interesting data structure in Redis. A probabilistic balanced structure giving O(log n) insert, delete, and range queries. The sorted set uses a dual index: skip list for ordering + HashMap for O(1) score-by-member lookup.

### Task 8: Skip list implementation

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/SkipList.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/SkipListTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.random.Random

class SkipListTest {

    @Test
    fun `insert and forward traversal is sorted`() {
        val sl = SkipList()
        sl.insert(3.0, "c")
        sl.insert(1.0, "a")
        sl.insert(2.0, "b")
        val all = sl.range(0, Long.MAX_VALUE)
        assertThat(all.map { it.member }).containsExactly("a", "b", "c")
    }

    @Test
    fun `delete preserves order`() {
        val sl = SkipList()
        sl.insert(1.0, "a")
        sl.insert(2.0, "b")
        sl.insert(3.0, "c")
        sl.delete("b")
        assertThat(sl.range(0, Long.MAX_VALUE).map { it.member }).containsExactly("a", "c")
    }

    @Test
    fun `range query by score`() {
        val sl = SkipList()
        for (i in 1..10) sl.insert(i.toDouble(), "m$i")
        val result = sl.rangeByScore(3.0, 7.0)
        assertThat(result.map { it.member }).containsExactly("m3", "m4", "m5", "m6", "m7")
    }

    @Test
    fun `rank returns 0-based position`() {
        val sl = SkipList()
        sl.insert(10.0, "a")
        sl.insert(20.0, "b")
        sl.insert(30.0, "c")
        assertThat(sl.rank("a")).isEqualTo(0)
        assertThat(sl.rank("b")).isEqualTo(1)
        assertThat(sl.rank("c")).isEqualTo(2)
        assertThat(sl.rank("missing")).isNull()
    }

    @Test
    fun `duplicate scores sort lexicographically by member`() {
        val sl = SkipList()
        sl.insert(1.0, "banana")
        sl.insert(1.0, "apple")
        sl.insert(1.0, "cherry")
        assertThat(sl.range(0, Long.MAX_VALUE).map { it.member })
            .containsExactly("apple", "banana", "cherry")
    }

    @Test
    fun `update score for existing member`() {
        val sl = SkipList()
        sl.insert(1.0, "a")
        sl.insert(2.0, "b")
        sl.insert(10.0, "a") // update a's score
        assertThat(sl.range(0, Long.MAX_VALUE).map { it.member }).containsExactly("b", "a")
    }

    @Test
    fun `stress test — 100k random inserts stay sorted`() {
        val sl = SkipList()
        val rng = Random(42)
        repeat(100_000) { i ->
            sl.insert(rng.nextDouble(-1000.0, 1000.0), "m$i")
        }
        val all = sl.range(0, Long.MAX_VALUE)
        for (i in 1 until all.size) {
            val prev = all[i - 1]
            val curr = all[i]
            assertThat(prev.score < curr.score ||
                    (prev.score == curr.score && prev.member < curr.member))
                .isTrue()
        }
    }
}
```

- [ ] **Step 2: Run tests — verify fail (class not found)**

- [ ] **Step 3: Implement SkipList**

```kotlin
package dynacache.engine

import kotlin.random.Random

data class SkipListEntry(val score: Double, val member: String)

class SkipList(
    private val maxLevel: Int = 32,
    private val p: Double = 0.25,
) {
    class Node(
        var score: Double,
        var member: String,
        level: Int,
    ) {
        val forward: Array<Node?> = arrayOfNulls(level)
        val span: IntArray = IntArray(level) // span[i] = elements skipped at level i
    }

    private val header = Node(Double.NEGATIVE_INFINITY, "", maxLevel)
    private var level = 1
    private var length = 0L
    private val rng = Random(System.nanoTime())

    private fun randomLevel(): Int {
        var lvl = 1
        while (lvl < maxLevel && rng.nextDouble() < p) lvl++
        return lvl
    }

    /** Compare by (score, member) — the skip list's total ordering. */
    private fun lt(s1: Double, m1: String, s2: Double, m2: String): Boolean =
        s1 < s2 || (s1 == s2 && m1 < m2)

    fun insert(score: Double, member: String): Boolean {
        val update = arrayOfNulls<Node>(maxLevel)
        val rank = IntArray(maxLevel)
        var x = header

        for (i in level - 1 downTo 0) {
            rank[i] = if (i == level - 1) 0 else rank[i + 1]
            while (x.forward[i] != null && lt(x.forward[i]!!.score, x.forward[i]!!.member, score, member)) {
                rank[i] += x.span[i]
                x = x.forward[i]!!
            }
            update[i] = x
        }

        // If member already exists with different score, delete first then re-insert
        // (handled by caller via delete+insert, or we can check here)

        val newLevel = randomLevel()
        if (newLevel > level) {
            for (i in level until newLevel) {
                rank[i] = 0
                update[i] = header
                header.span[i] = length.toInt()
            }
            level = newLevel
        }

        val node = Node(score, member, newLevel)
        for (i in 0 until newLevel) {
            node.forward[i] = update[i]!!.forward[i]
            update[i]!!.forward[i] = node

            node.span[i] = (update[i]!!.span[i]) - (rank[0] - rank[i])
            update[i]!!.span[i] = (rank[0] - rank[i]) + 1
        }
        for (i in newLevel until level) {
            update[i]!!.span[i]++
        }

        length++
        return true
    }

    fun delete(member: String): Boolean {
        // Find the node by traversing — we need the score to locate it
        val score = findScore(member) ?: return false
        val update = arrayOfNulls<Node>(maxLevel)
        var x = header
        for (i in level - 1 downTo 0) {
            while (x.forward[i] != null && lt(x.forward[i]!!.score, x.forward[i]!!.member, score, member)) {
                x = x.forward[i]!!
            }
            update[i] = x
        }
        x = x.forward[0] ?: return false
        if (x.member != member) return false

        for (i in 0 until level) {
            if (update[i]!!.forward[i] == x) {
                update[i]!!.span[i] += x.span[i] - 1
                update[i]!!.forward[i] = x.forward[i]
            } else {
                update[i]!!.span[i]--
            }
        }
        while (level > 1 && header.forward[level - 1] == null) level--
        length--
        return true
    }

    private fun findScore(member: String): Double? {
        // Linear scan at level 0 — O(n). The SortedSet wrapper maintains a HashMap for O(1).
        var x = header.forward[0]
        while (x != null) {
            if (x.member == member) return x.score
            x = x.forward[0]
        }
        return null
    }

    /** Range by 0-based rank indices. */
    fun range(start: Long, stop: Long): List<SkipListEntry> {
        val s = if (start < 0) maxOf(0, length + start) else start
        val e = if (stop < 0) length + stop else minOf(stop, length - 1)
        if (s > e || s >= length) return emptyList()

        val result = mutableListOf<SkipListEntry>()
        var traversed = -1L
        var x = header
        for (i in level - 1 downTo 0) {
            while (x.forward[i] != null && traversed + x.span[i] < s) {
                traversed += x.span[i]
                x = x.forward[i]!!
            }
        }
        x = x.forward[0] ?: return result
        traversed++

        while (traversed <= e && x != null) {
            result.add(SkipListEntry(x.score, x.member))
            x = x.forward[0] ?: break
            traversed++
        }
        return result
    }

    /** Range by score bounds (inclusive). */
    fun rangeByScore(min: Double, max: Double): List<SkipListEntry> {
        val result = mutableListOf<SkipListEntry>()
        var x = header
        for (i in level - 1 downTo 0) {
            while (x.forward[i] != null && x.forward[i]!!.score < min) {
                x = x.forward[i]!!
            }
        }
        x = x.forward[0] ?: return result
        while (x.score <= max) {
            result.add(SkipListEntry(x.score, x.member))
            x = x.forward[0] ?: break
        }
        return result
    }

    /** 0-based rank of member, or null if not found. */
    fun rank(member: String): Long? {
        val score = findScore(member) ?: return null
        var r = 0L
        var x = header
        for (i in level - 1 downTo 0) {
            while (x.forward[i] != null && lt(x.forward[i]!!.score, x.forward[i]!!.member, score, member)) {
                r += x.span[i]
                x = x.forward[i]!!
            }
        }
        x = x.forward[0] ?: return null
        return if (x.member == member) r else null
    }

    fun size(): Long = length
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): skip list — insert/delete/range/rangeByScore/rank with span tracking"
```

### Task 9: Sorted Set commands wired to skip list + HashMap

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/SortedSetValue.kt`
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/SortedSetCommandTest.kt`

- [ ] **Step 1: Write SortedSetValue — the dual index wrapper**

```kotlin
package dynacache.engine

/**
 * Dual-index sorted set: SkipList for ordering, HashMap for O(1) score lookup.
 * This is the same design Redis uses internally for ZSet.
 */
class SortedSetValue {
    val skipList = SkipList()
    val dict = HashMap<String, Double>()  // member → score

    fun add(score: Double, member: String): Boolean {
        val existing = dict[member]
        if (existing != null) {
            if (existing == score) return false  // no change
            skipList.delete(member)
        }
        skipList.insert(score, member)
        dict[member] = score
        return existing == null  // true if new member
    }

    fun remove(member: String): Boolean {
        val score = dict.remove(member) ?: return false
        skipList.delete(member)
        return true
    }

    fun score(member: String): Double? = dict[member]
    fun rank(member: String): Long? = skipList.rank(member)
    fun size(): Long = skipList.size()
    fun range(start: Long, stop: Long) = skipList.range(start, stop)
    fun rangeByScore(min: Double, max: Double) = skipList.rangeByScore(min, max)

    fun incrBy(member: String, increment: Double): Double {
        val oldScore = dict[member] ?: 0.0
        val newScore = oldScore + increment
        if (dict.containsKey(member)) skipList.delete(member)
        skipList.insert(newScore, member)
        dict[member] = newScore
        return newScore
    }
}
```

- [ ] **Step 2: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SortedSetCommandTest {
    private val engine = DataEngine()

    @Test
    fun `ZADD and ZRANGE returns sorted order`() {
        engine.execute(Command.ZAdd("z", listOf(3.0 to "c", 1.0 to "a", 2.0 to "b")))
        val res = engine.execute(Command.ZRange("z", 0, -1)) as Response.ArrayReply
        val members = res.values!!.map { String((it as Response.BulkString).value!!) }
        assertThat(members).containsExactly("a", "b", "c")
    }

    @Test
    fun `ZRANGE WITHSCORES includes scores`() {
        engine.execute(Command.ZAdd("z", listOf(1.5 to "a")))
        val res = engine.execute(Command.ZRange("z", 0, -1, withScores = true)) as Response.ArrayReply
        assertThat(res.values).hasSize(2) // [member, score]
    }

    @Test
    fun `ZRANGEBYSCORE filters by score`() {
        engine.execute(Command.ZAdd("z", listOf(1.0 to "a", 5.0 to "b", 10.0 to "c")))
        val res = engine.execute(Command.ZRangeByScore("z", 2.0, 8.0)) as Response.ArrayReply
        assertThat(res.values).hasSize(1)
        assertThat(String((res.values!![0] as Response.BulkString).value!!)).isEqualTo("b")
    }

    @Test
    fun `ZRANK returns 0-based rank`() {
        engine.execute(Command.ZAdd("z", listOf(10.0 to "a", 20.0 to "b", 30.0 to "c")))
        assertThat(engine.execute(Command.ZRank("z", "b"))).isEqualTo(Response.IntegerReply(1))
    }

    @Test
    fun `ZRANK missing member returns NIL`() {
        engine.execute(Command.ZAdd("z", listOf(1.0 to "a")))
        assertThat((engine.execute(Command.ZRank("z", "missing")) as Response.BulkString).value).isNull()
    }

    @Test
    fun `ZSCORE returns member score`() {
        engine.execute(Command.ZAdd("z", listOf(3.14 to "pi")))
        val res = engine.execute(Command.ZScore("z", "pi")) as Response.BulkString
        assertThat(String(res.value!!)).isEqualTo("3.14")
    }

    @Test
    fun `ZADD updates score for existing member`() {
        engine.execute(Command.ZAdd("z", listOf(1.0 to "a", 2.0 to "b")))
        engine.execute(Command.ZAdd("z", listOf(10.0 to "a")))
        // a should now be after b
        val res = engine.execute(Command.ZRange("z", 0, -1)) as Response.ArrayReply
        val members = res.values!!.map { String((it as Response.BulkString).value!!) }
        assertThat(members).containsExactly("b", "a")
    }

    @Test
    fun `ZREM removes member`() {
        engine.execute(Command.ZAdd("z", listOf(1.0 to "a", 2.0 to "b")))
        engine.execute(Command.ZRem("z", listOf("a")))
        assertThat(engine.execute(Command.ZCard("z"))).isEqualTo(Response.IntegerReply(1))
    }

    @Test
    fun `ZINCRBY increments score`() {
        engine.execute(Command.ZAdd("z", listOf(5.0 to "a")))
        val res = engine.execute(Command.ZIncrBy("z", 3.0, "a")) as Response.BulkString
        assertThat(String(res.value!!)).isEqualTo("8.0")
    }

    @Test
    fun `ZREVRANGE returns reverse order`() {
        engine.execute(Command.ZAdd("z", listOf(1.0 to "a", 2.0 to "b", 3.0 to "c")))
        val res = engine.execute(Command.ZRevRange("z", 0, -1)) as Response.ArrayReply
        val members = res.values!!.map { String((it as Response.BulkString).value!!) }
        assertThat(members).containsExactly("c", "b", "a")
    }
}
```

- [ ] **Step 3: Run tests — verify fail**
- [ ] **Step 4: Implement ZSet commands in DataEngine**

Route all Z* commands. Storage is `SortedSetValue`. Each handler follows the same pattern: lazy expiry check, type check for ZSET, delegate to `SortedSetValue` methods, convert results to RESP responses.

ZRANGE returns `ArrayReply` of `BulkString` members. If `withScores=true`, interleave member and score (as string): `[member, scoreStr, member, scoreStr, ...]`. ZREVRANGE returns the range in reverse. ZSCORE returns score as a BulkString (Redis convention).

- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(engine): Sorted Set — skip list + HashMap dual index, all Z* commands"
```

---

## Sub-phase 1D: Timer Wheel + TTL Commands

**Concept:** Hierarchical timer wheel — O(1) insert/cancel/expire for key TTLs. Replaces naive sweep approaches. Learn the Varghese & Lauck (1987) algorithm: multiple wheel levels where lower levels cascade into higher levels, like a clock.

### Task 10: Hierarchical timer wheel

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/TimerWheel.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/TimerWheelTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TimerWheelTest {
    @Test
    fun `fires on time`() {
        val fired = mutableListOf<String>()
        val wheel = TimerWheel(tickMs = 100, onExpire = { fired.add(it) })

        wheel.schedule("k1", deadlineMs = 500)
        wheel.advance(toMs = 499)
        assertThat(fired).isEmpty()

        wheel.advance(toMs = 500)
        assertThat(fired).containsExactly("k1")
    }

    @Test
    fun `no early fire`() {
        val fired = mutableListOf<String>()
        val wheel = TimerWheel(tickMs = 100, onExpire = { fired.add(it) })
        wheel.schedule("k1", deadlineMs = 10_000)

        wheel.advance(toMs = 9_999)
        assertThat(fired).isEmpty()
    }

    @Test
    fun `cancel prevents fire`() {
        val fired = mutableListOf<String>()
        val wheel = TimerWheel(tickMs = 100, onExpire = { fired.add(it) })
        wheel.schedule("k1", deadlineMs = 500)
        wheel.cancel("k1")
        wheel.advance(toMs = 1000)
        assertThat(fired).isEmpty()
    }

    @Test
    fun `reschedule updates deadline`() {
        val fired = mutableListOf<String>()
        val wheel = TimerWheel(tickMs = 100, onExpire = { fired.add(it) })
        wheel.schedule("k1", deadlineMs = 500)
        wheel.schedule("k1", deadlineMs = 1000)  // reschedule

        wheel.advance(toMs = 600)
        assertThat(fired).isEmpty()  // old deadline passed, should NOT fire

        wheel.advance(toMs = 1000)
        assertThat(fired).containsExactly("k1")
    }

    @Test
    fun `ordering — earlier deadlines fire first`() {
        val fired = mutableListOf<String>()
        val wheel = TimerWheel(tickMs = 100, onExpire = { fired.add(it) })
        wheel.schedule("k3", deadlineMs = 300)
        wheel.schedule("k1", deadlineMs = 100)
        wheel.schedule("k2", deadlineMs = 200)

        wheel.advance(toMs = 300)
        assertThat(fired).containsExactly("k1", "k2", "k3")
    }

    @Test
    fun `high volume — 1M keys all fire correctly`() {
        var fireCount = 0L
        val tickMs = 10L
        val wheel = TimerWheel(tickMs = tickMs, onExpire = { fireCount++ })

        for (i in 0 until 1_000_000) {
            wheel.schedule("k$i", deadlineMs = (i % 10_000 + 1).toLong() * tickMs)
        }
        wheel.advance(toMs = 10_001 * tickMs)
        assertThat(fireCount).isEqualTo(1_000_000L)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**

- [ ] **Step 3: Implement TimerWheel**

A hierarchical timer wheel with 3 levels. Each level has 256 slots. Level 0 covers 256 ticks (finest resolution). Level 1 covers 256 × 256 ticks. Level 2 covers 256 × 256 × 256 ticks (covers TTLs up to ~18 hours at 1ms tick, or ~46 hours at 100ms tick).

```kotlin
package dynacache.engine

class TimerWheel(
    private val tickMs: Long = 1,
    private val onExpire: (String) -> Unit,
) {
    private val WHEEL_SIZE = 256
    private val LEVELS = 3

    // wheels[level][slot] = set of (key, deadlineMs)
    private val wheels: Array<Array<MutableMap<String, Long>>> = Array(LEVELS) {
        Array(WHEEL_SIZE) { mutableMapOf() }
    }

    // key → (level, slot) for O(1) cancel
    private val index = HashMap<String, Pair<Int, Int>>()

    private var currentMs = 0L

    fun schedule(key: String, deadlineMs: Long) {
        cancel(key)  // remove old entry if exists
        val (level, slot) = slotFor(deadlineMs)
        wheels[level][slot][key] = deadlineMs
        index[key] = level to slot
    }

    fun cancel(key: String) {
        val (level, slot) = index.remove(key) ?: return
        wheels[level][slot].remove(key)
    }

    fun advance(toMs: Long) {
        while (currentMs < toMs) {
            currentMs += tickMs
            // Fire level 0 slot
            val slot0 = ((currentMs / tickMs) % WHEEL_SIZE).toInt()

            // Cascade higher levels when lower level wraps around
            for (level in 1 until LEVELS) {
                val divisor = tickMs * pow256(level)
                if (currentMs % divisor == 0L) {
                    val slot = ((currentMs / divisor) % WHEEL_SIZE).toInt()
                    // Re-insert entries from this higher-level slot into lower levels
                    val entries = wheels[level][slot].toMap()
                    wheels[level][slot].clear()
                    for ((key, deadline) in entries) {
                        index.remove(key)
                        if (deadline <= currentMs) {
                            onExpire(key)
                        } else {
                            schedule(key, deadline)
                        }
                    }
                }
            }

            // Fire everything in level 0 current slot that has passed deadline
            val toFire = wheels[0][slot0].filter { (_, deadline) -> deadline <= currentMs }
            for ((key, _) in toFire) {
                wheels[0][slot0].remove(key)
                index.remove(key)
                onExpire(key)
            }
        }
    }

    private fun slotFor(deadlineMs: Long): Pair<Int, Int> {
        val delta = maxOf(0, (deadlineMs - currentMs) / tickMs)
        for (level in 0 until LEVELS) {
            val range = pow256(level + 1)
            if (delta < range) {
                val slot = ((deadlineMs / (tickMs * pow256(level))) % WHEEL_SIZE).toInt()
                return level to slot
            }
        }
        // Overflow: put in highest level, last slot
        return (LEVELS - 1) to (WHEEL_SIZE - 1)
    }

    private fun pow256(n: Int): Long {
        var result = 1L
        repeat(n) { result *= WHEEL_SIZE }
        return result
    }
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): hierarchical timer wheel — O(1) schedule/cancel, 3-level cascade"
```

### Task 11: Integrate timer wheel into DataEngine + TTL commands

**Files:**
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/TtlCommandTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TtlCommandTest {
    private var now = 1_000_000L
    private val engine = DataEngine(clock = { now })

    @Test
    fun `EXPIRE sets TTL, TTL returns remaining seconds`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        engine.execute(Command.Expire("k", 10))
        val ttl = engine.execute(Command.Ttl("k")) as Response.IntegerReply
        assertThat(ttl.value).isEqualTo(10)
    }

    @Test
    fun `TTL returns -1 for no expiry`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        assertThat(engine.execute(Command.Ttl("k"))).isEqualTo(Response.IntegerReply(-1))
    }

    @Test
    fun `TTL returns -2 for missing key`() {
        assertThat(engine.execute(Command.Ttl("missing"))).isEqualTo(Response.IntegerReply(-2))
    }

    @Test
    fun `PERSIST removes TTL`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        engine.execute(Command.Expire("k", 10))
        engine.execute(Command.Persist("k"))
        assertThat(engine.execute(Command.Ttl("k"))).isEqualTo(Response.IntegerReply(-1))
    }

    @Test
    fun `PEXPIRE and PTTL work in milliseconds`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        engine.execute(Command.PExpire("k", 5000))
        val pttl = engine.execute(Command.PTtl("k")) as Response.IntegerReply
        assertThat(pttl.value).isEqualTo(5000)
    }

    @Test
    fun `EXPIREAT sets absolute timestamp`() {
        engine.execute(Command.Set("k", "v".toByteArray()))
        val futureEpoch = now / 1000 + 60  // 60 seconds from now
        engine.execute(Command.ExpireAt("k", futureEpoch))
        val ttl = engine.execute(Command.Ttl("k")) as Response.IntegerReply
        assertThat(ttl.value).isEqualTo(60)
    }

    @Test
    fun `timer wheel actively expires keys`() {
        engine.execute(Command.Set("k", "v".toByteArray(), exSeconds = 2))
        now += 2001
        engine.advanceTimerWheel() // active expiry tick
        // key should be gone without needing a GET (lazy expiry)
        assertThat(engine.execute(Command.DbSize())).isEqualTo(Response.IntegerReply(0))
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Integrate TimerWheel into DataEngine**

Add a `TimerWheel` field to `DataEngine`. When SET EX/PX, EXPIRE, PEXPIRE, or EXPIREAT is called, schedule the key in the wheel. When PERSIST is called, cancel the wheel entry. Expose `advanceTimerWheel()` for the server to call periodically. The wheel's `onExpire` callback removes the key from the store.

Wire TTL/PTTL: compute remaining time from `entry.expiresAtMs - clock()`.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): TTL commands + timer wheel integration — EXPIRE/PEXPIRE/EXPIREAT/TTL/PTTL/PERSIST"
```

---

## Sub-phase 1E: Eviction — LRU + W-TinyLFU

**Concept:** Memory-bounded caching. LRU as baseline (simple, well-understood), then W-TinyLFU (the Caffeine algorithm) as the advanced policy. Learn the Count-Min Sketch (frequency estimation) and the admission window pattern.

### Task 12: LRU eviction with memory tracking

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/EvictionPolicy.kt`
- Modify: `DataEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/EvictionTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class EvictionTest {
    @Test
    fun `LRU evicts oldest-accessed key under pressure`() {
        val engine = DataEngine(maxKeys = 3, evictionPolicy = EvictionPolicyType.LRU)
        engine.execute(Command.Set("a", "1".toByteArray()))
        engine.execute(Command.Set("b", "2".toByteArray()))
        engine.execute(Command.Set("c", "3".toByteArray()))

        // Access a and b to make c the LRU
        engine.execute(Command.Get("a"))
        engine.execute(Command.Get("b"))

        // Trigger eviction by adding 4th key
        engine.execute(Command.Set("d", "4".toByteArray()))

        // c should be evicted
        assertThat((engine.execute(Command.Get("c")) as Response.BulkString).value).isNull()
        // a, b, d should survive
        assertThat((engine.execute(Command.Get("a")) as Response.BulkString).value).isNotNull()
        assertThat((engine.execute(Command.Get("b")) as Response.BulkString).value).isNotNull()
        assertThat((engine.execute(Command.Get("d")) as Response.BulkString).value).isNotNull()
    }

    @Test
    fun `eviction prefers expired keys over live keys`() {
        var now = 1000L
        val engine = DataEngine(maxKeys = 3, evictionPolicy = EvictionPolicyType.LRU, clock = { now })
        engine.execute(Command.Set("live1", "v".toByteArray()))
        engine.execute(Command.Set("expiring", "v".toByteArray(), exSeconds = 1))
        engine.execute(Command.Set("live2", "v".toByteArray()))

        now = 2001  // expiring key is now expired
        engine.execute(Command.Set("new", "v".toByteArray()))

        // expired key should be evicted, not a live one
        assertThat((engine.execute(Command.Get("live1")) as Response.BulkString).value).isNotNull()
        assertThat((engine.execute(Command.Get("live2")) as Response.BulkString).value).isNotNull()
        assertThat((engine.execute(Command.Get("new")) as Response.BulkString).value).isNotNull()
    }

    @Test
    fun `eviction does not corrupt remaining keys`() {
        val engine = DataEngine(maxKeys = 5, evictionPolicy = EvictionPolicyType.LRU)
        for (i in 0 until 10) {
            engine.execute(Command.Set("k$i", "value$i".toByteArray()))
        }
        // Some keys evicted, but remaining keys return correct values
        for (i in 0 until 10) {
            val res = engine.execute(Command.Get("k$i")) as Response.BulkString
            if (res.value != null) {
                assertThat(String(res.value)).isEqualTo("value$i")
            }
        }
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement EvictionPolicy and LRU**

```kotlin
// EvictionPolicy.kt
package dynacache.engine

enum class EvictionPolicyType { LRU, LFU, W_TINY_LFU }

interface EvictionPolicy {
    /** Called on every key access (read or write). */
    fun recordAccess(key: String)
    /** Called when a key is removed (expired, deleted, evicted). */
    fun recordRemoval(key: String)
    /** Select a victim key to evict. Returns null if nothing to evict. */
    fun selectVictim(store: Map<String, KeyEntry>, clock: () -> Long): String?
}

class LruPolicy : EvictionPolicy {
    override fun recordAccess(key: String) { /* LRU uses lastAccessMs on KeyEntry */ }
    override fun recordRemoval(key: String) { }
    override fun selectVictim(store: Map<String, KeyEntry>, clock: () -> Long): String? {
        val now = clock()
        // First: evict any expired key
        val expired = store.entries.firstOrNull { (_, e) ->
            e.expiresAtMs != -1L && now >= e.expiresAtMs
        }
        if (expired != null) return expired.key

        // Then: LRU — sample 5 random keys, evict the one with oldest lastAccessMs
        val sample = store.entries.shuffled().take(5)
        return sample.minByOrNull { it.value.lastAccessMs }?.key
    }
}
```

Add `maxKeys` and `evictionPolicy` parameters to `DataEngine`. Before each write, if `store.size >= maxKeys`, call `evictionPolicy.selectVictim()` and remove the victim.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): LRU eviction — sample-based with expired-first priority"
```

### Task 13: Count-Min Sketch (frequency estimation)

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/FrequencySketch.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/FrequencySketchTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FrequencySketchTest {
    @Test
    fun `frequency tracks access count approximately`() {
        val sketch = FrequencySketch(capacity = 1024)
        repeat(10) { sketch.increment("hot") }
        sketch.increment("cold")
        assertThat(sketch.frequency("hot")).isGreaterThan(sketch.frequency("cold"))
    }

    @Test
    fun `frequency is capped at 15`() {
        val sketch = FrequencySketch(capacity = 1024)
        repeat(1000) { sketch.increment("k") }
        assertThat(sketch.frequency("k")).isLessThanOrEqualTo(15)
    }

    @Test
    fun `reset halves all counters`() {
        val sketch = FrequencySketch(capacity = 1024)
        repeat(10) { sketch.increment("k") }
        val before = sketch.frequency("k")
        sketch.reset()
        assertThat(sketch.frequency("k")).isLessThanOrEqualTo(before / 2 + 1)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement FrequencySketch**

A 4-bit Count-Min Sketch following Caffeine's design. 4 hash functions, counters stored as 4-bit nibbles packed into a LongArray. Maximum counter value is 15. When total increments exceed `capacity * 10`, all counters are halved (the "reset" or "aging" step that prevents frequency fossilization).

```kotlin
package dynacache.engine

class FrequencySketch(capacity: Int) {
    private val tableSize = nextPowerOfTwo(maxOf(1, capacity))
    private val tableMask = tableSize - 1
    private val table = LongArray(tableSize)
    private var additions = 0
    private val resetThreshold = capacity * 10

    fun increment(key: String) {
        val hash = spread(key.hashCode())
        val start = (hash and 3) shl 2 // which 4-bit counter within the long

        for (i in 0 until 4) {
            val index = indexOf(hash, i)
            val offset = offsetOf(hash, i)
            val count = (table[index] ushr offset) and 0xFL
            if (count < 15L) {
                table[index] = table[index] + (1L shl offset)
            }
        }
        if (++additions >= resetThreshold) reset()
    }

    fun frequency(key: String): Int {
        val hash = spread(key.hashCode())
        var min = Int.MAX_VALUE
        for (i in 0 until 4) {
            val index = indexOf(hash, i)
            val offset = offsetOf(hash, i)
            val count = ((table[index] ushr offset) and 0xFL).toInt()
            min = minOf(min, count)
        }
        return min
    }

    fun reset() {
        for (i in table.indices) {
            table[i] = (table[i] ushr 1) and 0x7777777777777777L
        }
        additions /= 2
    }

    private fun indexOf(hash: Int, i: Int): Int = ((hash ushr (16 * (i / 2))) + i) and tableMask
    private fun offsetOf(hash: Int, i: Int): Int = ((hash ushr (2 + i * 8)) and 0xF) shl 2

    private fun spread(x: Int): Int {
        var h = x
        h = h xor (h ushr 16)
        h *= 0x45d9f3b.toInt()
        h = h xor (h ushr 16)
        return h
    }

    private fun nextPowerOfTwo(n: Int): Int {
        var v = n - 1
        v = v or (v ushr 1)
        v = v or (v ushr 2)
        v = v or (v ushr 4)
        v = v or (v ushr 8)
        v = v or (v ushr 16)
        return v + 1
    }
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): Count-Min Sketch (FrequencySketch) — 4-bit counters with aging"
```

### Task 14: W-TinyLFU eviction policy

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/WTinyLfuPolicy.kt`
- Modify: `dynacache-engine/src/test/kotlin/dynacache/engine/EvictionTest.kt`

- [ ] **Step 1: Add W-TinyLFU test**

```kotlin
@Test
fun `W-TinyLFU admits frequently accessed keys over infrequent ones`() {
    val engine = DataEngine(maxKeys = 10, evictionPolicy = EvictionPolicyType.W_TINY_LFU)

    // Fill cache with "cold" keys
    for (i in 0 until 10) {
        engine.execute(Command.Set("cold$i", "v".toByteArray()))
    }

    // Create a "hot" access pattern — access cold0 many times to build frequency
    repeat(20) { engine.execute(Command.Get("cold0")) }

    // Now insert a new key — it should evict a cold key, not cold0
    engine.execute(Command.Set("new", "v".toByteArray()))
    assertThat((engine.execute(Command.Get("cold0")) as Response.BulkString).value).isNotNull()
}
```

- [ ] **Step 2: Run test — verify fail**
- [ ] **Step 3: Implement WTinyLfuPolicy**

The policy uses the FrequencySketch for admission decisions. When eviction is needed:
1. Evict expired keys first
2. Pick a candidate from the admission window (1% of capacity, most recent entries, LRU)
3. Pick the victim from the main space (oldest accessed)
4. Compare frequency(candidate) vs frequency(victim)
5. If candidate frequency > victim frequency: admit candidate, evict victim. Else evict candidate.

For simplicity in this phase, approximate the window/main split using `lastAccessMs` thresholds rather than maintaining separate data structures.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): W-TinyLFU eviction — frequency sketch admission + LRU main space"
```

---

## Sub-phase 1F: RESP Server + MULTI/EXEC + Lua

**Concept:** Wire everything to the network. Build a RESP2 parser/encoder (the Redis wire protocol), a Netty pipeline, and serve commands over TCP. Then add MULTI/EXEC transactions and embedded Lua scripting.

### Task 15: RESP2 codec — parser and encoder

**Files:**
- Create: `dynacache-server/src/main/kotlin/dynacache/server/RespDecoder.kt`
- Create: `dynacache-server/src/main/kotlin/dynacache/server/RespEncoder.kt`
- Create: `dynacache-server/src/test/kotlin/dynacache/server/RespCodecTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RespCodecTest {
    @Test
    fun `decode array command — SET foo bar`() {
        val input = "*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n"
        val result = RespDecoder.decode(input.toByteArray())
        assertThat(result).isEqualTo(listOf("SET", "foo", "bar"))
    }

    @Test
    fun `decode inline command — PING`() {
        val input = "PING\r\n"
        val result = RespDecoder.decodeInline(input.toByteArray())
        assertThat(result).isEqualTo(listOf("PING"))
    }

    @Test
    fun `encode simple string`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.SimpleString("OK"))
        assertThat(String(encoded)).isEqualTo("+OK\r\n")
    }

    @Test
    fun `encode error`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.Error("ERR", "unknown command"))
        assertThat(String(encoded)).isEqualTo("-ERR unknown command\r\n")
    }

    @Test
    fun `encode integer`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.IntegerReply(42))
        assertThat(String(encoded)).isEqualTo(":42\r\n")
    }

    @Test
    fun `encode bulk string`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.BulkString("hello".toByteArray()))
        assertThat(String(encoded)).isEqualTo("\$5\r\nhello\r\n")
    }

    @Test
    fun `encode nil bulk string`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.BulkString(null))
        assertThat(String(encoded)).isEqualTo("\$-1\r\n")
    }

    @Test
    fun `encode array`() {
        val encoded = RespEncoder.encode(dynacache.engine.Response.ArrayReply(listOf(
            dynacache.engine.Response.BulkString("a".toByteArray()),
            dynacache.engine.Response.BulkString("b".toByteArray()),
        )))
        assertThat(String(encoded)).isEqualTo("*2\r\n\$1\r\na\r\n\$1\r\nb\r\n")
    }

    @Test
    fun `fuzz — random bytes do not crash decoder`() {
        val rng = java.util.Random(42)
        repeat(10_000) {
            val bytes = ByteArray(rng.nextInt(100) + 1)
            rng.nextBytes(bytes)
            // Should return null or throw a controlled parse exception, never crash
            runCatching { RespDecoder.decode(bytes) }
        }
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement RespDecoder and RespEncoder**

`RespDecoder`: Parse RESP2 from a byte buffer. Handle `*` (array), `$` (bulk string), `+` (simple string), `-` (error), `:` (integer). Also handle inline format (plain space-separated text). Return `List<String>` (the command tokens).

`RespEncoder`: Convert `Response` sealed class to RESP2 bytes. Straightforward format writing.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): RESP2 codec — decode (array + inline) and encode all types"
```

### Task 16: Command parser — RESP tokens to Command objects

**Files:**
- Create: `dynacache-server/src/main/kotlin/dynacache/server/CommandParser.kt`
- Create: `dynacache-server/src/test/kotlin/dynacache/server/CommandParserTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.server

import dynacache.engine.Command
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CommandParserTest {
    @Test
    fun `parses SET key value`() {
        val cmd = CommandParser.parse(listOf("SET", "foo", "bar"))
        assertThat(cmd).isInstanceOf(Command.Set::class.java)
        val set = cmd as Command.Set
        assertThat(set.key).isEqualTo("foo")
        assertThat(String(set.value)).isEqualTo("bar")
    }

    @Test
    fun `parses SET key value NX EX 30`() {
        val cmd = CommandParser.parse(listOf("SET", "k", "v", "NX", "EX", "30")) as Command.Set
        assertThat(cmd.nx).isTrue()
        assertThat(cmd.exSeconds).isEqualTo(30)
    }

    @Test
    fun `parses ZADD key score member score member`() {
        val cmd = CommandParser.parse(listOf("ZADD", "z", "1.5", "alice", "2.0", "bob")) as Command.ZAdd
        assertThat(cmd.members).containsExactly(1.5 to "alice", 2.0 to "bob")
    }

    @Test
    fun `unknown command returns error`() {
        val cmd = CommandParser.parse(listOf("NONSENSE"))
        assertThat(cmd).isNull()
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement CommandParser**

A large `when` on the uppercase command name. Parse arguments positionally. For SET, parse optional NX/XX/EX/PX flags by scanning remaining tokens. For ZADD, parse score/member pairs. Case-insensitive command names.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): CommandParser — RESP tokens to engine Command objects"
```

### Task 17: Netty server — single-node RESP endpoint

**Files:**
- Create: `dynacache-server/src/main/kotlin/dynacache/server/RespServerHandler.kt`
- Create: `dynacache-server/src/main/kotlin/dynacache/server/RespServer.kt`
- Create: `dynacache-server/src/main/kotlin/dynacache/server/Main.kt`

- [ ] **Step 1: Implement Netty RESP pipeline**

`RespServer`: Netty `ServerBootstrap` with NIO transport. Pipeline: `ByteBuf → RespDecoder (ByteToMessageDecoder) → RespServerHandler → RespEncoder (MessageToByteEncoder)`.

`RespServerHandler`: Receives decoded command tokens, calls `CommandParser.parse()`, then `dataEngine.execute()`, writes back the encoded response.

`Main`: Parse config (port, max-keys, eviction policy), start `RespServer`.

- [ ] **Step 2: Build and start**

```bash
cd "$ROOT" && $MVN package -q -DskipTests
java -jar dynacache-server/target/dynacache-server-*.jar --port 6379
```

- [ ] **Step 3: Manual test with redis-cli**

```bash
redis-cli -p 6379
> PING
PONG
> SET foo bar
OK
> GET foo
"bar"
> SET counter 0
OK
> INCR counter
(integer) 1
> ZADD leaderboard 100 alice 200 bob
(integer) 2
> ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "bob"
4) "200"
> EXPIRE foo 10
(integer) 1
> TTL foo
(integer) 10
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(server): Netty RESP server — single-node redis-cli compatible"
```

### Task 18: MULTI/EXEC transactions

**Files:**
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/RespServerHandler.kt`
- Create: `dynacache-server/src/test/kotlin/dynacache/server/TransactionTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.server

import dynacache.engine.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TransactionTest {
    @Test
    fun `MULTI-EXEC executes commands atomically and returns results`() {
        val engine = DataEngine()
        val handler = TransactionHandler(engine)

        assertThat(handler.handle(listOf("MULTI"))).isEqualTo(Response.SimpleString("OK"))
        assertThat(handler.handle(listOf("SET", "a", "1"))).isEqualTo(Response.SimpleString("QUEUED"))
        assertThat(handler.handle(listOf("SET", "b", "2"))).isEqualTo(Response.SimpleString("QUEUED"))
        assertThat(handler.handle(listOf("GET", "a"))).isEqualTo(Response.SimpleString("QUEUED"))

        val res = handler.handle(listOf("EXEC")) as Response.ArrayReply
        assertThat(res.values).hasSize(3)
        assertThat(res.values!![0]).isEqualTo(Response.OK)
        assertThat(res.values!![1]).isEqualTo(Response.OK)
        assertThat((res.values!![2] as Response.BulkString).value).isEqualTo("1".toByteArray())
    }

    @Test
    fun `DISCARD clears queue`() {
        val engine = DataEngine()
        val handler = TransactionHandler(engine)

        handler.handle(listOf("MULTI"))
        handler.handle(listOf("SET", "a", "1"))
        handler.handle(listOf("DISCARD"))

        assertThat((engine.execute(Command.Get("a")) as Response.BulkString).value).isNull()
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement TransactionHandler**

Per-connection state: `inMulti: Boolean`, `queue: MutableList<List<String>>`. On MULTI: set flag. On subsequent commands: queue them, return "QUEUED". On EXEC: parse and execute all queued commands sequentially, collect results into ArrayReply. On DISCARD: clear queue and flag.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): MULTI/EXEC/DISCARD transactions — queued atomic execution"
```

### Task 19: Lua scripting with LuaJ

**Files:**
- Create: `dynacache-server/src/main/kotlin/dynacache/server/LuaScriptEngine.kt`
- Create: `dynacache-server/src/test/kotlin/dynacache/server/LuaScriptTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.server

import dynacache.engine.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LuaScriptTest {
    @Test
    fun `EVAL with redis_call SET and GET`() {
        val engine = DataEngine()
        val lua = LuaScriptEngine(engine)

        val result = lua.eval(
            script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            keys = listOf("mykey"),
            args = listOf("myvalue"),
        )
        assertThat((result as Response.BulkString).value).isEqualTo("myvalue".toByteArray())
    }

    @Test
    fun `EVAL arithmetic — atomic increment`() {
        val engine = DataEngine()
        engine.execute(Command.Set("counter", "10".toByteArray()))
        val lua = LuaScriptEngine(engine)

        val result = lua.eval(
            script = """
                local val = tonumber(redis.call('GET', KEYS[1]))
                redis.call('SET', KEYS[1], tostring(val + 1))
                return val + 1
            """.trimIndent(),
            keys = listOf("counter"),
            args = emptyList(),
        )
        assertThat(result).isEqualTo(Response.IntegerReply(11))
    }

    @Test
    fun `EVAL cannot access os or io`() {
        val engine = DataEngine()
        val lua = LuaScriptEngine(engine)

        val result = lua.eval(
            script = "return os.execute('ls')",
            keys = emptyList(),
            args = emptyList(),
        )
        assertThat(result).isInstanceOf(Response.Error::class.java)
    }

    @Test
    fun `EVAL KEYS and ARGV are correct`() {
        val engine = DataEngine()
        val lua = LuaScriptEngine(engine)

        val result = lua.eval(
            script = "return KEYS[1] .. ':' .. ARGV[1]",
            keys = listOf("hello"),
            args = listOf("world"),
        )
        assertThat((result as Response.BulkString).value).isEqualTo("hello:world".toByteArray())
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement LuaScriptEngine**

Create a sandboxed LuaJ environment:
1. Create `Globals` from `JsePlatform.standardGlobals()`
2. Remove `os`, `io`, `debug`, `loadfile`, `dofile` from globals
3. Override `math.random` and `math.randomseed` to error
4. Register `redis.call` and `redis.pcall` as Lua functions that parse args → `CommandParser.parse()` → `engine.execute()` → convert Response back to Lua values
5. Set `KEYS` and `ARGV` as Lua tables before execution
6. Execute the script string
7. Convert Lua return value to Response (number → IntegerReply, string → BulkString, table → ArrayReply, nil → NIL)

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): Lua scripting (LuaJ) — EVAL with sandboxed redis.call, KEYS/ARGV"
```

### Task 20: Wire MULTI/EXEC and EVAL into the Netty pipeline + final P1 integration test

**Files:**
- Modify: `RespServerHandler.kt`
- Create: `dynacache-server/src/test/kotlin/dynacache/server/IntegrationTest.kt`

- [ ] **Step 1: Wire transaction and Lua handlers into the Netty pipeline**

The `RespServerHandler` should maintain per-connection `TransactionHandler` state. When a command is `EVAL`, delegate to `LuaScriptEngine`. When in MULTI mode, queue everything except EXEC/DISCARD.

- [ ] **Step 2: Write integration test**

```kotlin
package dynacache.server

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.AfterEach
import java.net.Socket

class IntegrationTest {
    private val port = 16379
    private val server = RespServer(port = port)

    init { server.start() }

    @AfterEach
    fun teardown() { server.stop() }

    @Test
    fun `end-to-end SET GET over TCP`() {
        Socket("localhost", port).use { sock ->
            val out = sock.getOutputStream()
            val inp = sock.getInputStream()

            // SET foo bar
            out.write("*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n".toByteArray())
            out.flush()
            val setResp = readLine(inp)
            assert(setResp == "+OK")

            // GET foo
            out.write("*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n".toByteArray())
            out.flush()
            val lenLine = readLine(inp)  // $3
            val valLine = readLine(inp)  // bar
            assert(valLine == "bar")
        }
    }

    private fun readLine(inp: java.io.InputStream): String {
        val sb = StringBuilder()
        while (true) {
            val b = inp.read()
            if (b == '\r'.code) { inp.read(); return sb.toString() }
            if (b == -1) return sb.toString()
            sb.append(b.toChar())
        }
    }
}
```

- [ ] **Step 3: Run all tests**

```bash
cd "$ROOT" && $MVN test -q
```

Expected: ALL PASS.

- [ ] **Step 4: Manual smoke test with redis-cli**

Verify: SET, GET, INCR, HSET, HGETALL, LPUSH, LRANGE, ZADD, ZRANGE, EXPIRE, TTL, MULTI/EXEC, EVAL.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): P1 complete — single-node Redis-compatible cache with RESP, MULTI/EXEC, Lua"
```

---

## P1 Exit Criteria

All of the following must be true:
- [ ] `mvn test` — all tests green
- [ ] `redis-cli` SET/GET/INCR works
- [ ] `redis-cli` HSET/HGETALL works
- [ ] `redis-cli` LPUSH/LRANGE/RPOP works
- [ ] `redis-cli` ZADD/ZRANGE/ZRANGEBYSCORE/ZRANK works
- [ ] `redis-cli` EXPIRE/TTL — key expires on time
- [ ] `redis-cli` MULTI/EXEC — atomic batch
- [ ] `redis-cli` EVAL — Lua scripting with redis.call
- [ ] Timer wheel actively expires keys (not just lazy)
- [ ] Eviction works under memory pressure (LRU or W-TinyLFU)
- [ ] No data corruption after eviction or expiry

When all green: **P1 is done.** Move to P2 (Distribution).

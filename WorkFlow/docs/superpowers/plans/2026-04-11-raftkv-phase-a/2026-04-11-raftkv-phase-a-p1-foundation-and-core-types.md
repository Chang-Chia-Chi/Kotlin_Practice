# RaftKV Phase A — P1: Foundation and Core Data Model

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap the RaftKV Maven multi-module project and define every pure data type from §3 of the Phase A design doc. After this plan, `raftkv-core` compiles, has the complete type skeleton, and `RaftNode.step()` is a stub that returns empty effects.

**Architecture:** Standalone Maven project at `C:\Users\maxch\OneDrive\文件\GitHub\Kotlin_Practice\RaftKV\` (bash: `/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV/`). `raftkv-core` module with **zero runtime dependencies beyond `kotlin-stdlib`** — this is the compile-time enforcement of the pure-core invariant. Types follow Raft paper Figure 2 conventions (1-based log indexing, term/index on every log entry).

**Tech Stack:** Kotlin 2.2.x, JDK 21, Maven 3.9.8 (at `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`), JUnit 5 (Jupiter), AssertJ for assertions.

**Plan conventions:**
- `$MVN` is shorthand for `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`. All Maven invocations use this path.
- `$PROJECT_DIR` is shorthand for `/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV`. All file paths after Task 1 are relative to this directory.
- Every task ends with a commit. Commit messages follow conventional-commits style (no body required for these foundation commits).

---

## Task 1: Create Maven project skeleton and initialize git

**Files:**
- Create: `$PROJECT_DIR/pom.xml`
- Create: `$PROJECT_DIR/.gitignore`
- Create: `$PROJECT_DIR/README.md`

- [ ] **Step 1: Create project directory and initialize git**

```bash
mkdir -p "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
git init
```

Expected: `Initialized empty Git repository in ...`

- [ ] **Step 2: Write the parent pom.xml**

Create `pom.xml` in `$PROJECT_DIR`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <groupId>raftkv</groupId>
    <artifactId>raftkv-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>RaftKV (parent)</name>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <kotlin.version>2.2.20</kotlin.version>
        <kotlin.code.style>official</kotlin.code.style>
        <junit.version>5.11.0</junit.version>
        <assertj.version>3.26.3</assertj.version>
    </properties>

    <modules>
        <module>raftkv-core</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-stdlib</artifactId>
                <version>${kotlin.version}</version>
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
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.jetbrains.kotlin</groupId>
                    <artifactId>kotlin-maven-plugin</artifactId>
                    <version>${kotlin.version}</version>
                    <executions>
                        <execution>
                            <id>compile</id>
                            <phase>compile</phase>
                            <goals><goal>compile</goal></goals>
                        </execution>
                        <execution>
                            <id>test-compile</id>
                            <phase>test-compile</phase>
                            <goals><goal>test-compile</goal></goals>
                        </execution>
                    </executions>
                    <configuration>
                        <jvmTarget>21</jvmTarget>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>3.5.0</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] **Step 3: Write .gitignore**

Create `.gitignore` in `$PROJECT_DIR`:

```
target/
*.iml
.idea/
.vscode/
*.class
*.log
.DS_Store
```

- [ ] **Step 4: Write README.md**

Create `README.md` in `$PROJECT_DIR`:

```markdown
# RaftKV

A Raft-based replicated key-value store built from scratch in Kotlin.

Learning project — see `docs/design.md` for the Phase A design.

## Build

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn package
```
```

- [ ] **Step 5: Verify the parent POM parses (non-recursive, child module does not yet exist)**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -N help:effective-pom -q > /dev/null
echo "exit: $?"
```

Expected: `exit: 0`. The `-N` flag tells Maven to process only the reactor root (this `pom.xml`), skipping the declared `raftkv-core` child that we will create in Task 2. If exit is non-zero, there is a syntax error in the parent POM — fix it before committing.

- [ ] **Step 6: Commit**

```bash
git add pom.xml .gitignore README.md
git commit -m "chore: initialize RaftKV parent Maven project"
```

---

## Task 2: Create raftkv-core module skeleton

**Files:**
- Create: `raftkv-core/pom.xml`
- Create: `raftkv-core/src/main/kotlin/raftkv/core/.gitkeep`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/.gitkeep`

- [ ] **Step 1: Create directory structure**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
mkdir -p raftkv-core/src/main/kotlin/raftkv/core
mkdir -p raftkv-core/src/test/kotlin/raftkv/core
touch raftkv-core/src/main/kotlin/raftkv/core/.gitkeep
touch raftkv-core/src/test/kotlin/raftkv/core/.gitkeep
```

- [ ] **Step 2: Write raftkv-core/pom.xml**

Create `raftkv-core/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>raftkv</groupId>
        <artifactId>raftkv-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>raftkv-core</artifactId>
    <name>RaftKV Core (pure — zero runtime deps)</name>

    <dependencies>
        <dependency>
            <groupId>org.jetbrains.kotlin</groupId>
            <artifactId>kotlin-stdlib</artifactId>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <sourceDirectory>src/main/kotlin</sourceDirectory>
        <testSourceDirectory>src/test/kotlin</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-maven-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Verify the empty module compiles**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core compile
```

Expected: `BUILD SUCCESS` — empty module compiles clean.

- [ ] **Step 4: Commit**

```bash
git add raftkv-core/
git commit -m "chore(core): add empty raftkv-core module"
```

---

## Task 3: Define primitive ID types (NodeId, ClientId, RequestId)

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Ids.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/IdsTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/IdsTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class IdsTest {
    @Test
    fun `NodeId equality and hashing work`() {
        assertThat(NodeId(1)).isEqualTo(NodeId(1))
        assertThat(NodeId(1)).isNotEqualTo(NodeId(2))
        assertThat(NodeId(1).hashCode()).isEqualTo(NodeId(1).hashCode())
    }

    @Test
    fun `ClientId is a string wrapper`() {
        assertThat(ClientId("alice")).isEqualTo(ClientId("alice"))
        assertThat(ClientId("alice")).isNotEqualTo(ClientId("bob"))
    }

    @Test
    fun `RequestId combines clientId and sequence number`() {
        val r = RequestId(ClientId("alice"), 42L)
        assertThat(r.clientId).isEqualTo(ClientId("alice"))
        assertThat(r.seq).isEqualTo(42L)
        assertThat(r).isEqualTo(RequestId(ClientId("alice"), 42L))
        assertThat(r).isNotEqualTo(RequestId(ClientId("alice"), 43L))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=IdsTest
```

Expected: Compilation failure — `NodeId`, `ClientId`, `RequestId` are unresolved references.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Ids.kt`:

```kotlin
package raftkv.core

@JvmInline
value class NodeId(val value: Int)

@JvmInline
value class ClientId(val value: String)

data class RequestId(val clientId: ClientId, val seq: Long)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=IdsTest
```

Expected: `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Ids.kt raftkv-core/src/test/kotlin/raftkv/core/IdsTest.kt
git commit -m "feat(core): add NodeId, ClientId, RequestId value types"
```

---

## Task 4: Define Command and KvOperation

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Command.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/CommandTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/CommandTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CommandTest {
    @Test
    fun `KvOperation Get has a key`() {
        val op = KvOperation.Get("foo")
        assertThat(op.key).isEqualTo("foo")
    }

    @Test
    fun `KvOperation Put equality compares key and value bytes`() {
        val a = KvOperation.Put("k", byteArrayOf(1, 2, 3))
        val b = KvOperation.Put("k", byteArrayOf(1, 2, 3))
        assertThat(a).isEqualTo(b)
    }

    @Test
    fun `KvOperation Cas holds expected and new bytes`() {
        val op = KvOperation.Cas("k", expected = null, new = byteArrayOf(9))
        assertThat(op.expected).isNull()
        assertThat(op.new).containsExactly(9)
    }

    @Test
    fun `Command NoOp is a singleton`() {
        assertThat(Command.NoOp).isSameAs(Command.NoOp)
    }

    @Test
    fun `Command KvOp wraps operation and request id`() {
        val reqId = RequestId(ClientId("alice"), 1L)
        val cmd = Command.KvOp(KvOperation.Get("foo"), reqId)
        assertThat(cmd.op).isEqualTo(KvOperation.Get("foo"))
        assertThat(cmd.requestId).isEqualTo(reqId)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=CommandTest
```

Expected: Compilation failure — `KvOperation` and `Command` are unresolved references.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Command.kt`:

```kotlin
package raftkv.core

sealed class KvOperation {
    data class Get(val key: String) : KvOperation()

    class Put(val key: String, val value: ByteArray) : KvOperation() {
        override fun equals(other: Any?): Boolean =
            other is Put && key == other.key && value.contentEquals(other.value)
        override fun hashCode(): Int = 31 * key.hashCode() + value.contentHashCode()
        override fun toString(): String = "Put(key='$key', value=${value.contentToString()})"
    }

    data class Delete(val key: String) : KvOperation()

    class Cas(val key: String, val expected: ByteArray?, val new: ByteArray?) : KvOperation() {
        override fun equals(other: Any?): Boolean =
            other is Cas && key == other.key &&
            (expected?.contentEquals(other.expected) ?: (other.expected == null)) &&
            (new?.contentEquals(other.new) ?: (other.new == null))
        override fun hashCode(): Int {
            var h = key.hashCode()
            h = 31 * h + (expected?.contentHashCode() ?: 0)
            h = 31 * h + (new?.contentHashCode() ?: 0)
            return h
        }
        override fun toString(): String =
            "Cas(key='$key', expected=${expected?.contentToString()}, new=${new?.contentToString()})"
    }
}

sealed class Command {
    data class KvOp(val op: KvOperation, val requestId: RequestId) : Command()
    data object NoOp : Command()
}
```

**Rationale for custom equals/hashCode on Put and Cas:** `data class` with `ByteArray` fields uses reference equality by default, which breaks value semantics. We override to use `contentEquals`.

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=CommandTest
```

Expected: `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Command.kt raftkv-core/src/test/kotlin/raftkv/core/CommandTest.kt
git commit -m "feat(core): add Command and KvOperation sealed hierarchies"
```

---

## Task 5: Define LogEntry and Log

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Log.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/LogTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/LogTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class LogTest {
    private val reqId = RequestId(ClientId("c1"), 1L)

    private fun entry(term: Long, index: Long): LogEntry =
        LogEntry(term = term, index = index, command = Command.NoOp)

    @Test
    fun `empty log has lastIndex 0 and lastTerm 0`() {
        val log = Log()
        assertThat(log.lastIndex()).isEqualTo(0L)
        assertThat(log.lastTerm()).isEqualTo(0L)
    }

    @Test
    fun `append adds contiguous entries`() {
        val log = Log()
        log.append(listOf(entry(1, 1), entry(1, 2), entry(2, 3)))
        assertThat(log.lastIndex()).isEqualTo(3L)
        assertThat(log.lastTerm()).isEqualTo(2L)
    }

    @Test
    fun `append rejects non-contiguous entries`() {
        val log = Log()
        log.append(listOf(entry(1, 1)))
        assertThatThrownBy { log.append(listOf(entry(1, 3))) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `termAt returns the term at a given index`() {
        val log = Log()
        log.append(listOf(entry(1, 1), entry(2, 2), entry(2, 3)))
        assertThat(log.termAt(1)).isEqualTo(1L)
        assertThat(log.termAt(2)).isEqualTo(2L)
        assertThat(log.termAt(3)).isEqualTo(2L)
    }

    @Test
    fun `termAt returns null for out-of-range index`() {
        val log = Log()
        log.append(listOf(entry(1, 1)))
        assertThat(log.termAt(0)).isNull()
        assertThat(log.termAt(2)).isNull()
    }

    @Test
    fun `slice returns a subrange up to maxCount`() {
        val log = Log()
        log.append((1..5L).map { entry(1, it) })
        val slice = log.slice(fromIndex = 2, maxCount = 3)
        assertThat(slice.map { it.index }).containsExactly(2L, 3L, 4L)
    }

    @Test
    fun `slice returns empty when fromIndex exceeds lastIndex`() {
        val log = Log()
        log.append(listOf(entry(1, 1)))
        assertThat(log.slice(fromIndex = 5, maxCount = 10)).isEmpty()
    }

    @Test
    fun `truncateSuffixFrom removes entries at or after the given index`() {
        val log = Log()
        log.append((1..5L).map { entry(1, it) })
        log.truncateSuffixFrom(3)
        assertThat(log.lastIndex()).isEqualTo(2L)
    }

    @Test
    fun `LogEntry equality includes term, index, and command`() {
        val a = LogEntry(term = 1, index = 1, command = Command.NoOp)
        val b = LogEntry(term = 1, index = 1, command = Command.NoOp)
        assertThat(a).isEqualTo(b)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=LogTest
```

Expected: Compilation failure — `LogEntry` and `Log` are unresolved references.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Log.kt`:

```kotlin
package raftkv.core

/**
 * A single log entry. Index is 1-based (Raft paper convention).
 */
data class LogEntry(
    val term: Long,
    val index: Long,
    val command: Command,
)

/**
 * In-memory Raft log. Entries are 1-indexed. Phase A has no compaction —
 * all entries live in memory until snapshotting is added in P6.
 *
 * This class is NOT thread-safe; it is always accessed via the single
 * coroutine that owns the RaftNode core.
 */
class Log {
    private val entries: MutableList<LogEntry> = ArrayList()

    fun lastIndex(): Long = entries.size.toLong()

    fun lastTerm(): Long = entries.lastOrNull()?.term ?: 0L

    fun termAt(index: Long): Long? =
        if (index in 1L..lastIndex()) entries[(index - 1).toInt()].term else null

    fun append(newEntries: List<LogEntry>) {
        for (e in newEntries) {
            require(e.index == lastIndex() + 1) {
                "non-contiguous entry: got index ${e.index}, expected ${lastIndex() + 1}"
            }
            entries.add(e)
        }
    }

    fun truncateSuffixFrom(index: Long) {
        while (entries.isNotEmpty() && entries.last().index >= index) {
            entries.removeAt(entries.size - 1)
        }
    }

    fun slice(fromIndex: Long, maxCount: Int): List<LogEntry> {
        if (fromIndex > lastIndex() || fromIndex < 1) return emptyList()
        val start = (fromIndex - 1).toInt()
        val end = (start + maxCount).coerceAtMost(entries.size)
        return entries.subList(start, end).toList()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=LogTest
```

Expected: `Tests run: 9, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Log.kt raftkv-core/src/test/kotlin/raftkv/core/LogTest.kt
git commit -m "feat(core): add LogEntry and in-memory Log with contiguous append"
```

---

## Task 6: Define Raft RPCs

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Rpc.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/RpcTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/RpcTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RpcTest {
    @Test
    fun `RequestVote carries term, candidateId, and last-log info`() {
        val rv = RequestVote(term = 5, candidateId = NodeId(1), lastLogIndex = 10, lastLogTerm = 4)
        assertThat(rv.term).isEqualTo(5L)
        assertThat(rv.candidateId).isEqualTo(NodeId(1))
        assertThat(rv.lastLogIndex).isEqualTo(10L)
        assertThat(rv.lastLogTerm).isEqualTo(4L)
    }

    @Test
    fun `RequestVote is a RaftRpc`() {
        val rv: RaftRpc = RequestVote(1, NodeId(1), 0, 0)
        assertThat(rv).isInstanceOf(RaftRpc::class.java)
    }

    @Test
    fun `AppendEntries carries leader, prev-log info, entries, and leaderCommit`() {
        val entry = LogEntry(term = 2, index = 5, command = Command.NoOp)
        val ae = AppendEntries(
            term = 2, leaderId = NodeId(1),
            prevLogIndex = 4, prevLogTerm = 2,
            entries = listOf(entry), leaderCommit = 3
        )
        assertThat(ae.entries).containsExactly(entry)
        assertThat(ae.leaderCommit).isEqualTo(3L)
    }

    @Test
    fun `AppendEntriesResponse carries conflict info for fast backtracking`() {
        val resp = AppendEntriesResponse(
            term = 2, success = false, matchIndex = 0,
            conflictIndex = 5, conflictTerm = 1
        )
        assertThat(resp.success).isFalse()
        assertThat(resp.conflictIndex).isEqualTo(5L)
        assertThat(resp.conflictTerm).isEqualTo(1L)
    }

    @Test
    fun `InstallSnapshot carries lastIncluded info and data`() {
        val snap = InstallSnapshot(
            term = 5, leaderId = NodeId(1),
            lastIncludedIndex = 100, lastIncludedTerm = 4,
            offset = 0, data = byteArrayOf(1, 2, 3), done = true
        )
        assertThat(snap.lastIncludedIndex).isEqualTo(100L)
        assertThat(snap.done).isTrue()
        assertThat(snap.data).containsExactly(1, 2, 3)
    }

    @Test
    fun `all responses are RaftRpcResponse subtypes`() {
        val a: RaftRpcResponse = RequestVoteResponse(1, true)
        val b: RaftRpcResponse = AppendEntriesResponse(1, true, 5, null, null)
        val c: RaftRpcResponse = InstallSnapshotResponse(1)
        assertThat(a).isInstanceOf(RaftRpcResponse::class.java)
        assertThat(b).isInstanceOf(RaftRpcResponse::class.java)
        assertThat(c).isInstanceOf(RaftRpcResponse::class.java)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RpcTest
```

Expected: Compilation failure — none of the RPC types exist yet.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Rpc.kt`:

```kotlin
package raftkv.core

/** Marker base for all Raft RPC requests. */
sealed class RaftRpc

/** Marker base for all Raft RPC responses. */
sealed class RaftRpcResponse

// ---------- RequestVote ----------

data class RequestVote(
    val term: Long,
    val candidateId: NodeId,
    val lastLogIndex: Long,
    val lastLogTerm: Long,
) : RaftRpc()

data class RequestVoteResponse(
    val term: Long,
    val voteGranted: Boolean,
) : RaftRpcResponse()

// ---------- AppendEntries ----------

data class AppendEntries(
    val term: Long,
    val leaderId: NodeId,
    val prevLogIndex: Long,
    val prevLogTerm: Long,
    val entries: List<LogEntry>,
    val leaderCommit: Long,
) : RaftRpc()

data class AppendEntriesResponse(
    val term: Long,
    val success: Boolean,
    val matchIndex: Long,
    val conflictIndex: Long?,
    val conflictTerm: Long?,
) : RaftRpcResponse()

// ---------- InstallSnapshot ----------

class InstallSnapshot(
    val term: Long,
    val leaderId: NodeId,
    val lastIncludedIndex: Long,
    val lastIncludedTerm: Long,
    val offset: Long,
    val data: ByteArray,
    val done: Boolean,
) : RaftRpc() {
    override fun equals(other: Any?): Boolean =
        other is InstallSnapshot &&
            term == other.term && leaderId == other.leaderId &&
            lastIncludedIndex == other.lastIncludedIndex &&
            lastIncludedTerm == other.lastIncludedTerm &&
            offset == other.offset && done == other.done &&
            data.contentEquals(other.data)

    override fun hashCode(): Int {
        var h = term.hashCode()
        h = 31 * h + leaderId.hashCode()
        h = 31 * h + lastIncludedIndex.hashCode()
        h = 31 * h + lastIncludedTerm.hashCode()
        h = 31 * h + offset.hashCode()
        h = 31 * h + data.contentHashCode()
        h = 31 * h + done.hashCode()
        return h
    }
}

data class InstallSnapshotResponse(
    val term: Long,
) : RaftRpcResponse()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RpcTest
```

Expected: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Rpc.kt raftkv-core/src/test/kotlin/raftkv/core/RpcTest.kt
git commit -m "feat(core): add RequestVote, AppendEntries, InstallSnapshot RPCs"
```

---

## Task 7: Define Event sealed class

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Event.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/EventTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/EventTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class EventTest {
    @Test
    fun `RpcReceived carries sender and rpc`() {
        val rpc = RequestVote(1, NodeId(2), 0, 0)
        val evt = Event.RpcReceived(from = NodeId(2), rpc = rpc)
        assertThat(evt.from).isEqualTo(NodeId(2))
        assertThat(evt.rpc).isEqualTo(rpc)
    }

    @Test
    fun `RpcResponse carries sender and response`() {
        val resp = RequestVoteResponse(1, true)
        val evt = Event.RpcResponse(from = NodeId(3), response = resp)
        assertThat(evt.from).isEqualTo(NodeId(3))
        assertThat(evt.response).isEqualTo(resp)
    }

    @Test
    fun `ElectionTick and HeartbeatTick are singletons`() {
        assertThat(Event.ElectionTick).isSameAs(Event.ElectionTick)
        assertThat(Event.HeartbeatTick).isSameAs(Event.HeartbeatTick)
    }

    @Test
    fun `ClientCommand carries request id and command`() {
        val reqId = RequestId(ClientId("alice"), 1L)
        val cmd = Command.KvOp(KvOperation.Get("k"), reqId)
        val evt = Event.ClientCommand(id = reqId, command = cmd)
        assertThat(evt.id).isEqualTo(reqId)
        assertThat(evt.command).isEqualTo(cmd)
    }

    @Test
    fun `PersistAck carries lastPersistedIndex`() {
        val evt = Event.PersistAck(lastPersistedIndex = 42L)
        assertThat(evt.lastPersistedIndex).isEqualTo(42L)
    }

    @Test
    fun `ApplyAck carries lastAppliedIndex`() {
        val evt = Event.ApplyAck(lastAppliedIndex = 42L)
        assertThat(evt.lastAppliedIndex).isEqualTo(42L)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=EventTest
```

Expected: Compilation failure — `Event` is an unresolved reference.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Event.kt`:

```kotlin
package raftkv.core

/**
 * Every input to RaftNode.step() is an Event. This is the sole
 * interface between the outside world and the pure Raft core.
 */
sealed class Event {
    /** A peer sent us an RPC request. */
    data class RpcReceived(val from: NodeId, val rpc: RaftRpc) : Event()

    /** A peer responded to one of our outgoing RPCs. */
    data class RpcResponse(val from: NodeId, val response: RaftRpcResponse) : Event()

    /** Election timeout fired. */
    data object ElectionTick : Event()

    /** Leader heartbeat timer fired. */
    data object HeartbeatTick : Event()

    /** A client submitted a command. */
    data class ClientCommand(val id: RequestId, val command: Command) : Event()

    /** Log storage has durably persisted up to this index. */
    data class PersistAck(val lastPersistedIndex: Long) : Event()

    /** The state machine has applied up to this index. */
    data class ApplyAck(val lastAppliedIndex: Long) : Event()
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=EventTest
```

Expected: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Event.kt raftkv-core/src/test/kotlin/raftkv/core/EventTest.kt
git commit -m "feat(core): add Event sealed class as the sole core input"
```

---

## Task 8: Define Effects data class

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Effect.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/EffectTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/EffectTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class EffectTest {
    @Test
    fun `default Effects has empty lists and null options`() {
        val e = Effects()
        assertThat(e.sendMessages).isEmpty()
        assertThat(e.persistLog).isEmpty()
        assertThat(e.applyToStateMachine).isEmpty()
        assertThat(e.clientResponses).isEmpty()
        assertThat(e.persistState).isNull()
        assertThat(e.setElectionTimer).isNull()
        assertThat(e.setHeartbeatTimer).isNull()
        assertThat(e.cancelHeartbeatTimer).isFalse()
        assertThat(e.snapshotTrigger).isNull()
    }

    @Test
    fun `OutgoingRpc carries destination and rpc`() {
        val rpc = RequestVote(1, NodeId(1), 0, 0)
        val out = OutgoingRpc(to = NodeId(2), rpc = rpc)
        assertThat(out.to).isEqualTo(NodeId(2))
        assertThat(out.rpc).isEqualTo(rpc)
    }

    @Test
    fun `PersistentStateDelta carries new term and votedFor`() {
        val d = PersistentStateDelta(currentTerm = 5, votedFor = NodeId(1))
        assertThat(d.currentTerm).isEqualTo(5L)
        assertThat(d.votedFor).isEqualTo(NodeId(1))
    }

    @Test
    fun `PersistentStateDelta votedFor may be null`() {
        val d = PersistentStateDelta(currentTerm = 5, votedFor = null)
        assertThat(d.votedFor).isNull()
    }

    @Test
    fun `ClientResponse carries request id and index and result`() {
        val reqId = RequestId(ClientId("alice"), 1L)
        val cr = ClientResponse(requestId = reqId, logIndex = 10, result = ApplyResult.Ok(byteArrayOf(1)))
        assertThat(cr.requestId).isEqualTo(reqId)
        assertThat(cr.logIndex).isEqualTo(10L)
        assertThat(cr.result).isInstanceOf(ApplyResult.Ok::class.java)
    }

    @Test
    fun `Effects can be built with specific effects populated`() {
        val rpc = OutgoingRpc(NodeId(2), RequestVote(1, NodeId(1), 0, 0))
        val e = Effects(
            sendMessages = listOf(rpc),
            setElectionTimer = Duration.ofMillis(200),
        )
        assertThat(e.sendMessages).hasSize(1)
        assertThat(e.setElectionTimer).isEqualTo(Duration.ofMillis(200))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=EffectTest
```

Expected: Compilation failure — `Effects`, `OutgoingRpc`, `PersistentStateDelta`, `ClientResponse`, `ApplyResult` unresolved.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Effect.kt`:

```kotlin
package raftkv.core

import java.time.Duration

/**
 * Everything RaftNode.step() wants the outside world to do, as data.
 *
 * The runtime shell consumes Effects in a defined order (see spec §4.2):
 *   persistState → persistLog → sendMessages → applyToStateMachine →
 *   clientResponses → setElectionTimer/setHeartbeatTimer.
 *
 * All list fields default to empty; all option fields default to null.
 */
data class Effects(
    val sendMessages: List<OutgoingRpc> = emptyList(),
    val persistState: PersistentStateDelta? = null,
    val persistLog: List<LogEntry> = emptyList(),
    val applyToStateMachine: List<LogEntry> = emptyList(),
    val clientResponses: List<ClientResponse> = emptyList(),
    val setElectionTimer: Duration? = null,
    val setHeartbeatTimer: Duration? = null,
    val cancelHeartbeatTimer: Boolean = false,
    val snapshotTrigger: SnapshotTrigger? = null,
)

/** An RPC the core wants sent to a specific peer. */
data class OutgoingRpc(val to: NodeId, val rpc: RaftRpc)

/** Delta to be durably written to StateStorage. */
data class PersistentStateDelta(
    val currentTerm: Long,
    val votedFor: NodeId?,
)

/** Result the client should receive when a command has been applied. */
data class ClientResponse(
    val requestId: RequestId,
    val logIndex: Long,
    val result: ApplyResult,
)

/** Outcome of applying a command to the state machine. */
sealed class ApplyResult {
    data class Ok(val value: ByteArray?) : ApplyResult() {
        override fun equals(other: Any?): Boolean =
            other is Ok && (value?.contentEquals(other.value) ?: (other.value == null))
        override fun hashCode(): Int = value?.contentHashCode() ?: 0
    }
    data class CasFailed(val actual: ByteArray?) : ApplyResult() {
        override fun equals(other: Any?): Boolean =
            other is CasFailed && (actual?.contentEquals(other.actual) ?: (other.actual == null))
        override fun hashCode(): Int = actual?.contentHashCode() ?: 0
    }
    data object NotFound : ApplyResult()
}

/** The core requests a snapshot because the log has grown past a threshold. */
data class SnapshotTrigger(
    val lastIncludedIndex: Long,
    val lastIncludedTerm: Long,
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=EffectTest
```

Expected: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Effect.kt raftkv-core/src/test/kotlin/raftkv/core/EffectTest.kt
git commit -m "feat(core): add Effects, OutgoingRpc, PersistentStateDelta, ClientResponse, ApplyResult"
```

---

## Task 9: Define Role sealed class

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/Role.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/RoleTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/RoleTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RoleTest {
    @Test
    fun `Follower is a singleton`() {
        val r: Role = Role.Follower
        assertThat(r).isSameAs(Role.Follower)
    }

    @Test
    fun `Candidate tracks votes received`() {
        val c = Role.Candidate(votesReceived = setOf(NodeId(1), NodeId(2)))
        assertThat(c.votesReceived).containsExactlyInAnyOrder(NodeId(1), NodeId(2))
    }

    @Test
    fun `Leader carries LeaderState with per-peer nextIndex and matchIndex`() {
        val ls = LeaderState(
            nextIndex = mapOf(NodeId(2) to 5L, NodeId(3) to 5L),
            matchIndex = mapOf(NodeId(2) to 0L, NodeId(3) to 0L),
        )
        val l = Role.Leader(leaderState = ls)
        assertThat(l.leaderState.nextIndex[NodeId(2)]).isEqualTo(5L)
        assertThat(l.leaderState.matchIndex[NodeId(3)]).isEqualTo(0L)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RoleTest
```

Expected: Compilation failure — `Role` and `LeaderState` unresolved.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/Role.kt`:

```kotlin
package raftkv.core

/** The role of a Raft node at a point in time. */
sealed class Role {
    data object Follower : Role()
    data class Candidate(val votesReceived: Set<NodeId>) : Role()
    data class Leader(val leaderState: LeaderState) : Role()
}

/**
 * Leader-only volatile state. Reinitialized on every election.
 *
 * - nextIndex: next log index the leader will send to each follower (initialized to leader.lastIndex + 1)
 * - matchIndex: highest log index known to be replicated on each follower (initialized to 0)
 */
data class LeaderState(
    val nextIndex: Map<NodeId, Long>,
    val matchIndex: Map<NodeId, Long>,
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RoleTest
```

Expected: `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/Role.kt raftkv-core/src/test/kotlin/raftkv/core/RoleTest.kt
git commit -m "feat(core): add Role sealed class and LeaderState"
```

---

## Task 10: Define PersistentState and VolatileState

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/RaftState.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/RaftStateTest.kt`

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/RaftStateTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RaftStateTest {
    @Test
    fun `PersistentState holds currentTerm, votedFor, and log`() {
        val log = Log()
        val ps = PersistentState(currentTerm = 5, votedFor = NodeId(1), log = log)
        assertThat(ps.currentTerm).isEqualTo(5L)
        assertThat(ps.votedFor).isEqualTo(NodeId(1))
        assertThat(ps.log).isSameAs(log)
    }

    @Test
    fun `PersistentState votedFor may be null`() {
        val ps = PersistentState(currentTerm = 0, votedFor = null, log = Log())
        assertThat(ps.votedFor).isNull()
    }

    @Test
    fun `VolatileState holds commitIndex and lastApplied`() {
        val vs = VolatileState(commitIndex = 10, lastApplied = 8)
        assertThat(vs.commitIndex).isEqualTo(10L)
        assertThat(vs.lastApplied).isEqualTo(8L)
    }

    @Test
    fun `default VolatileState has zero indices`() {
        val vs = VolatileState()
        assertThat(vs.commitIndex).isEqualTo(0L)
        assertThat(vs.lastApplied).isEqualTo(0L)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RaftStateTest
```

Expected: Compilation failure — `PersistentState` and `VolatileState` unresolved.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/RaftState.kt`:

```kotlin
package raftkv.core

/**
 * State that must survive restart. Matches Raft paper Figure 2.
 *
 * Fsync ordering: currentTerm, votedFor, and any newly appended log entries
 * MUST be durable on disk before the node sends a response that acknowledges
 * them. The runtime shell enforces this via effect dispatch order.
 */
class PersistentState(
    var currentTerm: Long,
    var votedFor: NodeId?,
    val log: Log,
)

/**
 * State that is rebuilt from persistent state on restart.
 */
class VolatileState(
    var commitIndex: Long = 0L,
    var lastApplied: Long = 0L,
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RaftStateTest
```

Expected: `Tests run: 4, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/RaftState.kt raftkv-core/src/test/kotlin/raftkv/core/RaftStateTest.kt
git commit -m "feat(core): add PersistentState and VolatileState"
```

---

## Task 11: Define RaftNode class with stub step()

**Files:**
- Create: `raftkv-core/src/main/kotlin/raftkv/core/RaftNode.kt`
- Create: `raftkv-core/src/test/kotlin/raftkv/core/RaftNodeStubTest.kt`

This task creates the `RaftNode` skeleton with a stub `step()` that returns `Effects()` unconditionally. All real protocol logic is implemented in P3 (Leader Election) and later plans. The stub exists so P2 (Test Harness) can instantiate and wire `RaftNode`s without blocking on protocol logic.

- [ ] **Step 1: Write the failing test**

Create `raftkv-core/src/test/kotlin/raftkv/core/RaftNodeStubTest.kt`:

```kotlin
package raftkv.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class RaftNodeStubTest {
    private val peers = listOf(NodeId(1), NodeId(2), NodeId(3))

    private fun newNode(): RaftNode = RaftNode(
        id = NodeId(1),
        peers = peers,
        persistentState = PersistentState(currentTerm = 0, votedFor = null, log = Log()),
        volatileState = VolatileState(),
        role = Role.Follower,
    )

    @Test
    fun `new node starts as follower`() {
        val node = newNode()
        assertThat(node.role).isSameAs(Role.Follower)
    }

    @Test
    fun `step on ElectionTick returns empty Effects in stub`() {
        val node = newNode()
        val effects = node.step(Event.ElectionTick, Instant.EPOCH)
        assertThat(effects).isEqualTo(Effects())
    }

    @Test
    fun `step on HeartbeatTick returns empty Effects in stub`() {
        val node = newNode()
        val effects = node.step(Event.HeartbeatTick, Instant.EPOCH)
        assertThat(effects).isEqualTo(Effects())
    }

    @Test
    fun `step on RpcReceived returns empty Effects in stub`() {
        val node = newNode()
        val rpc = RequestVote(1, NodeId(2), 0, 0)
        val effects = node.step(Event.RpcReceived(NodeId(2), rpc), Instant.EPOCH)
        assertThat(effects).isEqualTo(Effects())
    }

    @Test
    fun `peers contains the configured peer list`() {
        val node = newNode()
        assertThat(node.peers).containsExactlyInAnyOrderElementsOf(peers)
    }

    @Test
    fun `id is the configured id`() {
        val node = newNode()
        assertThat(node.id).isEqualTo(NodeId(1))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RaftNodeStubTest
```

Expected: Compilation failure — `RaftNode` is unresolved.

- [ ] **Step 3: Write the implementation**

Create `raftkv-core/src/main/kotlin/raftkv/core/RaftNode.kt`:

```kotlin
package raftkv.core

import java.time.Instant

/**
 * The pure Raft state machine.
 *
 * This is Layer 1 of the architecture — no I/O, no threads, no clocks,
 * no sockets, no files. The only way to interact with it is through
 * [step], which takes an [Event] and returns [Effects] describing what
 * the outside world should do.
 *
 * This class is NOT thread-safe; it is always accessed from a single
 * coroutine owned by the runtime shell.
 *
 * STUB: all protocol logic (elections, replication, safety invariants)
 * is implemented in P3 and later plans. For now [step] returns an empty
 * [Effects] for every event, so the test harness (P2) can wire up
 * clusters without depending on protocol logic.
 */
class RaftNode(
    val id: NodeId,
    val peers: List<NodeId>,
    val persistentState: PersistentState,
    val volatileState: VolatileState,
    var role: Role,
) {
    /**
     * Advance the state machine by one event. Returns the effects the
     * runtime shell should perform in response.
     *
     * STUB: returns empty Effects unconditionally. Real logic arrives
     * in P3 (leader election).
     */
    fun step(event: Event, now: Instant): Effects {
        return Effects()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test -Dtest=RaftNodeStubTest
```

Expected: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add raftkv-core/src/main/kotlin/raftkv/core/RaftNode.kt raftkv-core/src/test/kotlin/raftkv/core/RaftNodeStubTest.kt
git commit -m "feat(core): add RaftNode skeleton with stub step()"
```

---

## Task 12: Full-suite green check

**Purpose:** Before closing out P1, run the entire `raftkv-core` test suite to confirm every test we wrote in tasks 3-11 still passes. Catches accidental cross-task regressions.

- [ ] **Step 1: Run the full test suite**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core test
```

Expected: `BUILD SUCCESS`. The output should show all test classes running with zero failures:
- `IdsTest` — 3 tests
- `CommandTest` — 5 tests
- `LogTest` — 9 tests
- `RpcTest` — 6 tests
- `EventTest` — 6 tests
- `EffectTest` — 6 tests
- `RoleTest` — 3 tests
- `RaftStateTest` — 4 tests
- `RaftNodeStubTest` — 6 tests

**Total: 48 tests, 0 failures.**

- [ ] **Step 2: Verify no dependency creep in raftkv-core**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -pl raftkv-core dependency:tree -Dscope=runtime
```

Expected: The only runtime dependency is `org.jetbrains.kotlin:kotlin-stdlib`. Test dependencies (`junit-jupiter`, `assertj-core`) will also appear but they are scope=test, which is fine. If anything else appears under runtime scope, investigate before proceeding — the pure-core invariant has been violated.

- [ ] **Step 3: Tag the P1 completion**

```bash
cd "/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/RaftKV"
git tag -a phase-a-p1-complete -m "P1: Foundation and core data model complete"
```

Expected: No output. Verify with `git tag`:

```bash
git tag
```

Expected output: `phase-a-p1-complete`.

---

## What P1 Delivered

After this plan is complete:

- **Maven multi-module project** exists at `~/GitHub/Kotlin_Practice/RaftKV/` with a parent POM and one child module.
- **`raftkv-core` module** has zero runtime dependencies beyond `kotlin-stdlib` — the pure-core invariant is enforced at build time.
- **Every pure data type** from §3 of the design doc exists: `NodeId`, `ClientId`, `RequestId`, `KvOperation`, `Command`, `LogEntry`, `Log`, all three RPCs and their responses, `Event`, `Effects`, `OutgoingRpc`, `PersistentStateDelta`, `ClientResponse`, `ApplyResult`, `SnapshotTrigger`, `Role`, `LeaderState`, `PersistentState`, `VolatileState`, `RaftNode`.
- **48 unit tests** — one test class per file, every type has a smoke test or behavioral test, one regression gate via the full-suite check.
- **`RaftNode.step()` is a stub** that returns empty `Effects`. P2 (Test Harness) will instantiate `RaftNode`s without depending on real protocol logic; P3 (Leader Election) is the first plan that implements `step()` behavior.

---

## What P1 Did NOT Deliver

Explicitly deferred to later plans in Phase A:

- **Test harness** (`FakeNetwork`, `VirtualClock`, `InMemoryLog`, `InMemoryStateStore`, `TestCluster`) — P2
- **Runtime shell** (`RaftRuntime` event loop) — P2 and P3
- **Real protocol logic** (elections, replication, safety checks) — P3 onward
- **Persistence adapters** (`FileLogStorage`, `FileStateStorage`) — P5
- **Snapshots** — P6
- **Client API** — P7
- **Linearizability checker** — P8

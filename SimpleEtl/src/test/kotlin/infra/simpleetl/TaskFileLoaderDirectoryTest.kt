package infra.simpleetl

import infra.simpleetl.TaskFiles.dirOf
import infra.simpleetl.TaskFiles.edit
import infra.simpleetl.TaskFiles.load
import infra.simpleetl.TaskFiles.minimal
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Done-when: "one bad file out of ten prevents startup, and the report lists only that file's
 * errors."
 *
 * Both halves, and the second one is the one with a defect behind it. A loader that validates
 * cross-file rules by accumulating state - names seen, datasets seen - and then reports against
 * whichever file it happened to be holding will blame a good file for a bad one's mistake. The
 * operator reading that report at 03:00 edits the wrong file.
 *
 * [allTenValidFilesLoad] is the pairing that keeps the rejection honest, the same way the
 * canaries do for the eighteen rules: without it, a loader that rejected any directory of ten
 * files would pass the test below.
 */
class TaskFileLoaderDirectoryTest {

    @TempDir
    lateinit var root: Path

    private val goodNames = (1..9).map { "task-$it" }

    private fun goodFiles(): Array<Pair<String, String>> =
        goodNames.map { "$it.yaml" to minimal(it) }.toTypedArray()

    private val badFile = edit(minimal("broken-task"), "name: broken-task", "name: broken-task\nchunkSizeTypo: 3")

    @Test
    fun allTenValidFilesLoad() {
        val directory = dirOf(root, *goodFiles(), "tenth.yaml" to minimal("task-10"))

        val tasks = load(directory).tasksOrFail()

        assertThat(tasks.map { it.name })
            .containsExactlyInAnyOrderElementsOf(goodNames + "task-10")
    }

    @Test
    fun oneBadFileOutOfTenPreventsStartup() {
        val directory = dirOf(root, *goodFiles(), "broken.yaml" to badFile)

        val loaded = load(directory)

        assertThat(loaded.tasks)
            .describedAs("nine good files do not make a bad one loadable (spec 10, spec 8.5)")
            .isNull()
        assertThat(loaded.errors).isNotEmpty()
    }

    @Test
    fun theReportListsOnlyTheBadFilesErrors() {
        val directory = dirOf(root, *goodFiles(), "broken.yaml" to badFile)

        val loaded = load(directory)

        assertThat(loaded.errors).allSatisfy { assertThat(it.file).contains("broken.yaml") }
        assertThat(loaded.errors)
            .describedAs("the report must name the offending field, not merely the file")
            .anySatisfy { assertThat(it.message).contains("chunkSizeTypo") }

        goodNames.forEach { good ->
            assertThat(loaded.errors)
                .describedAs("a phantom error was reported against the untouched file %s", good)
                .noneMatch { it.file.contains(good) || it.message.contains(good) }
        }
    }

    /**
     * What a Kubernetes ConfigMap volume actually looks like. The task directory is not a clean
     * set of task files: the kubelet keeps `..data` and a timestamped `..2026_08_27_...`
     * directory beside them and swings a symlink to make an update atomic (spec 8.5). A scanner
     * that read everything it found would fail startup on the platform's own bookkeeping, and it
     * would do so only once deployed - never in a test that lays out a tidy directory.
     */
    @Test
    fun theScanTakesYamlAndYmlAndIgnoresDotPrefixedEntries() {
        val directory = dirOf(
            root,
            "task-1.yaml" to minimal("task-1"),
            "task-2.yml" to minimal("task-2"),
            ".hidden.yaml" to "this: is: not: a task file",
            "notes.txt" to "not a task file either",
        )

        val tasks = load(directory).tasksOrFail()

        assertThat(tasks.map { it.name }).containsExactlyInAnyOrder("task-1", "task-2")
    }

    /**
     * `ValidationError.line`, asserted in both directions, because one direction alone is not a
     * test of anything: a loader that always reported null passes "rule 2 reports null", and one
     * that always reported a number passes "rule 1 reports one".
     *
     * The split is not arbitrary and it is not a guess about the implementation. Jackson knows a
     * source position for every deserialisation failure, so rule 1 can carry one for free; the
     * semantic rules work on the deserialised object tree, which holds no positions, so they
     * report null. The field is nullable in spec 11.2 for exactly this reason. What this test
     * pins is that the free half stays free - it is the half that regresses silently, because
     * dropping it breaks no other assertion.
     */
    @Test
    fun aDeserialisationErrorCarriesAYamlLineAndASemanticErrorDoesNot() {
        val unknownField = edit(minimal("bad-field"), "name: bad-field", "name: bad-field\nnope: 1")
        val badName = edit(minimal("bad-name"), "name: bad-name", "name: Bad_Name")

        val fromJackson = load(dirOf(root, "a.yaml" to unknownField)).errors.first { "nope" in it.message }
        val fromRule2 = load(dirOf(root, "b.yaml" to badName)).errors.first { "Bad_Name" in it.message }

        assertThat(fromJackson.line)
            .describedAs("Jackson reports a position for every deserialisation failure")
            .isNotNull()
        assertThat(fromRule2.line)
            .describedAs("the semantic rules work on an object tree that holds no source positions")
            .isNull()
    }

    /**
     * The report is ordered by file name. Promised in `TaskFileLoader`'s KDoc and, until now,
     * asserted nowhere - which is the shape of claim this project has been caught by five times.
     *
     * The files are written `c`, `a`, `b` so that a loader emitting them in directory-iteration
     * or insertion order fails. Rule order *within* one file is the other half of that promise
     * and is still untested: pinning it would require encoding the loader's internal rule
     * sequence, which is not a contract anything outside the class depends on.
     */
    @Test
    fun theReportIsOrderedByFileName() {
        val directory = dirOf(
            root,
            "c.yaml" to edit(minimal("task-c"), "name: task-c", "name: task-c\nnope: 1"),
            "a.yaml" to edit(minimal("task-a"), "name: task-a", "name: task-a\nnope: 1"),
            "b.yaml" to edit(minimal("task-b"), "name: task-b", "name: task-b\nnope: 1"),
        )

        val files = load(directory).errors.map { it.file }

        assertThat(files).containsExactly("a.yaml", "b.yaml", "c.yaml")
    }

    /**
     * The loader's own constructor precondition, which no rule in spec 10 covers: `scratch` is
     * the reserved per-run working file (spec 7.1), so a deployment that also configured a Jdbi
     * bean called `scratch` would make rule 3 accept a name that means two different things.
     * P3's lesson - a public member with no test shipped a real defect while 134 tests were
     * green - is why it gets one line here rather than a mention in a report.
     */
    @Test
    fun theReservedScratchNameCannotAlsoBeAConfiguredDatasource() {
        assertThatThrownBy { TaskFiles.loaderWithDatasources(setOf(SCRATCH)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(SCRATCH)
    }
}

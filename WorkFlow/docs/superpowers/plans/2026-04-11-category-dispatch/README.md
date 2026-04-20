# Category-Based Dispatch Scheduling — Plan Index

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/superpowers/specs/2026-04-11-category-dispatch-design.md`

**Goal:** Let operators run different sets of dispatch configs on different cron schedules by tagging each config with a category and passing a category set through the existing `initialItem` channel.

**Architecture:** New `DispatchCategory` enum on `DispatchConfig`. `DispatchScheduler` grows one `@Scheduled` method per category that calls a shared private `trigger(categories: Set<DispatchCategory>)` helper. Helper encodes the set into `initialItem` JSON. `DispatchScatterHandler` parses the set out of `taskPayload` and passes it to a widened `findActiveConfigs(asOf, categories)` repository call. Empty set = no filter = all active configs. Zero changes to the workflow engine, worker, DSL, or anything outside `dispatch/`.

**Tech Stack:** Kotlin 2.3.x, Quarkus 3.x (`@Scheduled`), JDBI 3 (port only — no in-repo implementation), JUnit 5 + Mockito Kotlin + `kotlinx-coroutines-test`, Jackson for JSON payload encoding.

---

## Phases

Each phase is a separate file. Work them in order — later phases assume earlier ones have landed. Every phase ends in a green `mvn test` run and a commit.

| # | File | Scope | Commits |
|---|---|---|---|
| 1 | [phase1-model.md](phase1-model.md) | Add `DispatchCategory` enum, add `category` field to `DispatchConfig`, fix positional construction sites, update E2E fixture loader + JSON. | 1 |
| 2 | [phase2-repository.md](phase2-repository.md) | Widen `DispatchConfigRepository.findActiveConfigs` to take `categories: Set<DispatchCategory> = emptySet()`. Source-compatible via default arg. | 1 |
| 3 | [phase3-handler.md](phase3-handler.md) | `DispatchScatterHandler` parses `categories` from `taskPayload` and passes to `handleCronTrigger`. Add parsing-case tests to `DispatchHandlersTest`. | 1 |
| 4 | [phase4-scheduler.md](phase4-scheduler.md) | Replace single `@Scheduled` with N category methods + private `trigger(categories)` helper. Update `application.properties`. Add new `DispatchSchedulerTest`. | 1 |
| 5 | [phase5-e2e.md](phase5-e2e.md) | Add a category-scoped variant to `DispatchE2EHappyPathTest` that asserts only matching configs reach the join. | 1 |

## Enum value placeholders

The spec uses `URGENT`, `NORMAL`, `BACKGROUND` as **placeholder** enum values. If you have finalized business category names, pattern-replace them throughout every phase file **before starting Phase 1**. The plan's code blocks, property keys, test names, and commit messages all reference the placeholder names — a global find/replace across the plan directory is the intended workflow.

## Hard constraints from project CLAUDE.md and user feedback

- Maven command: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn` — never use system `mvn` or `./mvnw`.
- Never chain shell commands with `&&` or `;` — one shell command per invocation.
- Delete obsolete TODO comments as you find them; don't leave them behind.
- Limit the changes per step to keep review surface small.

## Non-goals (out of scope for every phase)

Per the spec's Non-Goals section:

- Per-category deadline, retry policy, or `BatchStatus` variant
- Per-category `WorkflowDefinition` DSL variant
- Admin API / DB-backed runtime mutation of categories
- Metrics or log tags keyed by category
- Overlap detection across schedules
- Jdbi repository SQL implementation (out of repo; port only)

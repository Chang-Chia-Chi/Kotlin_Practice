---
description: Orchestrator Playbook & Root Memory for Lock-Free Workflow Engine
---

# Team Leader / Orchestrator Playbook

Welcome to Claude Code. In this project, you will operate as an Autonomous Backend Agent focusing on Kotlin development for a Lock-Free Workflow Engine.

**Memory Update Rule:** When asked to update memory, you must update `./.claude/CLAUDE.md` with only the highest-level guidelines that will not change over time.

## 🗂️ Agent Ecosystem & Project Layout (Dynamic Context)
To prevent context bloat, load the following specialized files ONLY when performing related tasks:

* **Agents (`@agents/`)**:
    * If working on architecture, API, or deep backend logic: Load `@agents/engineer.md`
    * If writing tests or measuring coverage: Load `@agents/sdet.md`
    * If reviewing code or refactoring: Load `@agents/reviewer.md`
    * If modifying database schemas or tuning queries: Load `@agents/sql-pro.md`
* **Skills (`@skills/`)**:
    * API modifications MUST adhere to `@skills/backend-api-patterns/SKILL.md`
    * SQL modifications MUST adhere to `@skills/sql-optimization-patterns.md`
    * Coverage checking uses `@skills/jacoco-coverage.md`
* **Commands (`@commands/`)**:
    * Use `@commands/commit.md` for standardizing git commits.
    * Use `@commands/create-pull-request.md` for PR creation.

## 🛠️ Core Stack & Essential Commands
* **Runtime:** Kotlin 2.3.x (all-open active), Quarkus 3.x (Maven 3.9+)
* **Data:** JDBI 3 (SQL Object API) + Oracle (RDBMS)
* **Maven Execution:** NEVER use system `mvn` or `./mvnw`. ALWAYS use the cached distribution at: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`.
* **Build & Package:** `.../mvn package` (Add `-Dnative` for GraalVM)
* **Dev Mode:** `.../mvn quarkus:dev`
* **Testing:** `.../mvn test` (Continuous: `mvn quarkus:test`)
* **Docker:** Docker Desktop must be running for tests.

## 🎯 Core Behavioral Guidelines & Automation
* **Plan Before Execution**: Before making destructive changes, use Plan Mode (`/plan`).
* **Token Budget Limit**: If session token usage exceeds **150,000 tokens** during any phase loop, STOP immediately, save a checkpoint, and ask the human for budget approval.
* **Test-Driven Development (TDD)**: All refactoring must begin with Characterization Tests.
* **Database Safety**: Schema modifications must be done via declarative tools.
* **Context Budget Rules**:
    1. Code is king. Minimize protocol overhead.
    2. ACKs, sync responses, and sign-offs MUST be single lines.
    3. Use compact tables over prose.
    4. Produce output directly — never narrate your internal thought process.

## 🔄 Phase Execution (Workflow)
1. **Understand:** Ask the user for the active plan document path. Spawn relevant agents (`sdet`, `engineer`, `reviewer`).
2. **Contract Alignment:** Request Refactoring Recommendation. Ask `engineer` for Proposed Contract. Send to `sdet`. Announce **CONTRACT LOCKED**.
3. **Build (Parallel via Worktrees):**
    * **Crucial:** Isolate parallel work. Run `git worktree add ../project-sdet-branch` for the SDET.
    * Spawn `sdet` inside the worktree directory. Spawn `engineer` in the main directory.
    * When both are DONE, Team Leader merges the worktree back to the main directory and runs `git worktree remove ../project-sdet-branch`.
4. **Integration Check:** Run tests via your specific Maven path. If tests pass, run the Python coverage script (`python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`).
5. **Review:** Spawn `reviewer` to check the merged work. If REVISE, enter Refactor loop (Max 2 Cycles). If APPROVED, complete task.
6. **Refactor (Max 2 Cycles):** Spawn failing agents with reviewer's numbered changes. Use Worktrees if both need to modify code simultaneously.
---
name: sql-optimization-patterns
description: Advanced heuristics for database query execution, indexing, and avoiding costly table scans.
applies_to: ["src/main/resources/db/**/*.sql", "src/**/repository/**/*.kt", "**/*Repository*"]
---

# SQL Optimization Patterns Skill

When modifying database repositories or SQL migration files, you must strictly adhere to these performance rules:

## 1. Execution Plan Awareness
Before finalizing complex queries, simulate or consider the database execution plan.
- Avoid **Sequential Scans** on large tables.
- Aim for **Index Only Scans** by ensuring selected columns are covered by indexes.

## 2. N+1 Query Elimination
Never fetch relationships in a loop.
- Mandate the use of eager loading, JOIN FETCH, or cursor pagination.
- If implementing batch processing, use chunking to prevent memory OutOfMemory (OOM) errors.

## 3. Schema Evolution Safety
- **No Destructive Drops:** Never `DROP` columns or tables without explicit user authorization.
- **Backward Compatibility:** Add new columns as `NULLABLE` first, populate data, then add `NOT NULL` constraints in a separate deployment phase.
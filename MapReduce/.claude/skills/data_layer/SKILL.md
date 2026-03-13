# Data Access & RDBMS Standards

## JDBI Patterns

* **DAO Interfaces:** Use `@SqlQuery` and `@SqlUpdate` annotations.
* **Mapping:**
* Use Kotlin `data class` for row mapping.
* For 1:N relationships, use `reduceRows` or `Multimap` to avoid N+1 issues .
* Use `mapTo<T>()` for simple types.


* **Nullability:** Return `Optional<T>` for single rows; `List<T>` for multiple.

## Transaction Management

* Use `jdbi.inTransaction {... }` for multi-step operations.
* **Flyway:** All DDL must be in `src/main/resources/db/migration`. No `ddl-auto`.



## SQL Style

* Snake_case for DB columns; CamelCase for Kotlin properties .
* Use UUID v4 for public identifiers .

---
name: Kotlin Backend API Patterns
description: Production-grade backend API development standards and Kotlin design patterns
applies_to: ["src/main/kotlin/**/*.kt"]
---

# Kotlin Backend API Architecture Skill

When developing or modifying APIs in this project, you must strictly adhere to the following standards:

## 1. Idiomatic Kotlin & Architecture
* **Immutability First**: Always prefer `val` over `var`. Use immutable collections (`List`, `Map`) unless mutability is explicitly required.
* **Data Classes**: Use `data class` for all DTOs (Data Transfer Objects), Requests, and Responses to leverage built-in `copy()`, `equals()`, and `hashCode()` functions.
* **Asynchronous Processing**: Utilize Kotlin Coroutines (`suspend` functions and `Flow`) for non-blocking I/O operations and database calls instead of traditional thread-blocking patterns.

## 2. Performance & Database Access (ORMs)
* **Preventing N+1 Queries**: When fetching relationships, explicitly prevent N+1 issues.
    * If using JPA/Hibernate: Use `JOIN FETCH`, `@EntityGraph`, or DTO projections.
    * If using Exposed: Use `eager loading` or `preload`.
* **Parameterized Queries**: All raw SQL must be parameterized. Absolutely no string concatenation for SQL queries to prevent SQL injection.

## 3. Service-Repository Pattern
* **Strict Layering**:
    * **Controllers/Routes**: Only handle HTTP request/response formatting, JSON validation, and status codes.
    * **Services**: Encapsulate all business logic.
    * **Repositories**: Encapsulate all database interactions.
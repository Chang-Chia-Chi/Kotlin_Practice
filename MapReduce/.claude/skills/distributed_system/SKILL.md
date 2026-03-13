# Distributed Systems & Observability

## Observability (RED Method)

* **Tracing:** Ensure `trace_id` and `span_id` are propagated across service boundaries via W3C Traceparent .
* **Logging:** Use structured JSON logging via JBoss/SLF4J.
* **Metrics:** Instrument external calls with `@Counted` or `@Timed` using Micrometer/OTel .

## Resiliency Patterns

* **Fault Tolerance:** Wrap external HTTP/Kafka calls with `@Retry`, `@Timeout`, and `@CircuitBreaker` .
* **Idempotency:** Implement idempotency keys for POST operations to handle retries safely.
* **Messaging:** Follow the transactional producer pattern for Kafka to ensure "exactly-once" semantics .
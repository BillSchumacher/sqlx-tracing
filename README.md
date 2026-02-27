# sqlx-tracing

**sqlx-tracing** is a Rust library that provides OpenTelemetry-compatible tracing for SQLx database operations. It wraps SQLx connection pools and queries with tracing spans, enabling detailed observability of database interactions in distributed systems.

## Features

- **Automatic Tracing**: All SQLx queries executed through the provided pool are traced using [tracing](https://docs.rs/tracing) spans.
- **Lifecycle Tracing**: Pool operations (`acquire`, `close`) and transaction lifecycle (`begin`, `commit`, `rollback`) are instrumented with dedicated spans.
- **OpenTelemetry Integration**: Traces are compatible with OpenTelemetry, making it easy to export to collectors and observability platforms.
- **Error Recording**: Errors are automatically annotated with kind, message, and stacktrace in the tracing span.
- **Returned Rows**: The number of rows returned by queries is recorded for observability.
- **Pool Metrics**: Pool health methods (`size`, `num_idle`, `is_closed`) are exposed for monitoring.
- **Database Agnostic**: Supports both PostgreSQL and SQLite via feature flags.

## Usage

Add `sqlx-tracing` to your `Cargo.toml`:

```toml
[dependencies]
sqlx-tracing = "0.1"
sqlx = { version = "0.8", default-features = false, features = ["derive"] }
tracing = "0.1"
```

Enable the desired database feature:

- For PostgreSQL: `features = ["postgres"]`
- For SQLite: `features = ["sqlite"]`

Wrap your SQLx pool:

```rust,ignore
let pool = sqlx::PgPool::connect(&url).await?;
// the attributes will be resolved from the url
let traced_pool = sqlx_tracing::Pool::from(pool);
// or manually overwrite them
let traced_pool = sqlx_tracing::PoolBuilder::from(pool)
    .with_name("my-domain-database")
    .with_database("database")
    .with_host("somewhere")
    .with_port(1234)
    .build();
```

Use the traced pool as you would a normal SQLx pool:

```rust,ignore
let result: Option<i32> = sqlx::query_scalar("select 1")
    .fetch_optional(traced_pool)
    .await?;
```

This works also with pool connections:

```rust,ignore
let mut conn = traced_pool.acquire().await?;
let result: Option<i32> = sqlx::query_scalar("select 1")
    .fetch_optional(&mut conn)
    .await?;
```

### Pool Management

Check pool health and statistics:

```rust,ignore
let active = traced_pool.size();       // total connections (active + idle)
let idle = traced_pool.num_idle();     // idle connections
let closed = traced_pool.is_closed();  // whether close() has been called
```

Non-blocking connection acquisition:

```rust,ignore
if let Some(conn) = traced_pool.try_acquire() {
    // Use connection immediately
} else {
    // Pool is saturated, all connections in use
}
```

Graceful shutdown:

```rust,ignore
// Prevents new connections and waits for all active connections to return
traced_pool.close().await;
```

### Connection Health Checks

Ping a connection to verify it is still valid:

```rust,ignore
let mut conn = traced_pool.acquire().await?;
conn.ping().await?;
```

### Transactions

Begin a transaction from the pool:

```rust,ignore
let mut tx = traced_pool.begin().await?;
let result: Option<i32> = sqlx::query_scalar("select 1")
    .fetch_optional(&mut tx.executor())
    .await?;
tx.commit().await?;
```

Or from an existing connection:

```rust,ignore
let mut conn = traced_pool.acquire().await?;
let mut tx = conn.begin().await?;
sqlx::query("INSERT INTO users (name) VALUES ($1)")
    .bind("Alice")
    .execute(&mut tx.executor())
    .await?;
tx.commit().await?;
```

Transactions can also be explicitly rolled back:

```rust,ignore
let mut tx = traced_pool.begin().await?;
sqlx::query("INSERT INTO users (name) VALUES ($1)")
    .bind("Alice")
    .execute(&mut tx.executor())
    .await?;
// Discard the changes
tx.rollback().await?;
```

If a transaction is dropped without calling `commit` or `rollback`, it is
automatically rolled back.

All lifecycle operations (`acquire`, `begin`, `commit`, `rollback`, `close`,
`ping`) emit dedicated tracing spans with OpenTelemetry-compatible attributes.

### Bypassing Tracing

If you need to use SQLx features not yet supported by this crate (e.g. `COPY`,
`LISTEN/NOTIFY`, or other database-specific operations), you can access the
underlying `sqlx::Pool` directly:

```rust,ignore
// Via the inner() method
let raw_pool: &sqlx::PgPool = traced_pool.inner();
let mut conn = raw_pool.acquire().await?;

// Or via AsRef
let raw_pool: &sqlx::PgPool = traced_pool.as_ref();
```

Queries executed through the inner pool will not be traced.

## Security Considerations

### Query Text in Traces

By default, `sqlx-tracing` records the full SQL query text in the `db.query.text`
span field for every database operation. This follows
[OpenTelemetry Semantic Conventions for Database Spans](https://opentelemetry.io/docs/specs/semconv/database/database-spans/),
but means that:

- **SQL queries with inline values** (not parameterized) will have those values recorded in traces
- **Query structure reveals schema** — table names, column names, and query patterns are exposed
- **Connection topology is exposed** — database host, port, and name appear in every span

To disable query text recording:

```rust,ignore
let pool = sqlx_tracing::PoolBuilder::from(sqlx_pool)
    .with_query_text_recording(false)
    .build();
```

### Error Details in Traces

By default, error details including `Debug`-format stacktraces are recorded in
span fields. The `Debug` format of `sqlx::Error` can include connection strings
or internal database state. To disable detailed error recording:

```rust,ignore
let pool = sqlx_tracing::PoolBuilder::from(sqlx_pool)
    .with_error_detail_recording(false)
    .build();
```

When disabled, error spans will still record the error type (client/server) and
status code, but will omit the error message and stacktrace.

### Recommendations

- Always use **parameterized queries** to avoid exposing sensitive data in traces
- Ensure your **observability backend** has appropriate access controls
- Consider using **span processors** to redact sensitive data before export
- Disable query text and/or error detail recording if your traces flow to systems
  with different access controls than your database

## OpenTelemetry Integration

To export traces, set up an OpenTelemetry collector and configure the tracing subscriber with the appropriate layers. See the `tests/common.rs` for a full example using `opentelemetry`, `opentelemetry-otlp`, and `tracing-opentelemetry`.

## Testing

Integration tests are provided for both PostgreSQL and SQLite, using [testcontainers](https://docs.rs/testcontainers) and a local OpenTelemetry collector.

## License

Licensed under MIT.

## Contributing

Contributions and issues are welcome! Please open a PR or issue on GitHub.

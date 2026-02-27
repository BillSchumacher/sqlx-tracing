/// Macro to create a tracing span for a SQLx operation with OpenTelemetry-compatible fields.
///
/// - `$name`: The operation name (e.g., "sqlx.execute").
/// - `$statement`: The SQL statement being executed.
/// - `$attributes`: Connection or pool attributes for peer and db context.
///
/// This macro is used internally by the crate to instrument all major SQLx operations.
/// When `record_query_text` is disabled on the attributes, the `db.query.text` field
/// will be empty.
#[doc(hidden)]
#[macro_export]
macro_rules! instrument {
    ($name:expr, $statement:expr, $attributes:expr) => {
        tracing::info_span!(
            $name,
            // Database name (if available)
            "db.name" = $attributes.database,
            // Operation type (filled by SQLx or left empty)
            "db.operation" = ::tracing::field::Empty,
            // The SQL query text (conditionally recorded based on config)
            "db.query.text" = $attributes.record_query_text.then_some($statement),
            // Number of affected rows (to be filled after execution)
            "db.response.affected_rows" = ::tracing::field::Empty,
            // Number of returned rows (to be filled after execution)
            "db.response.returned_rows" = ::tracing::field::Empty,
            // Status code of the response (to be filled after execution)
            "db.response.status_code" = ::tracing::field::Empty,
            // Table name (optional, left empty)
            "db.sql.table" = ::tracing::field::Empty,
            // Database system (e.g., "postgresql", "sqlite")
            "db.system.name" = DB::SYSTEM,
            // Error type, message, and stacktrace (to be filled on error)
            "error.type" = ::tracing::field::Empty,
            "error.message" = ::tracing::field::Empty,
            "error.stacktrace" = ::tracing::field::Empty,
            // Peer (server) host and port
            "net.peer.name" = $attributes.host,
            "net.peer.port" = $attributes.port,
            // OpenTelemetry semantic fields
            "otel.kind" = "client",
            "otel.status_code" = ::tracing::field::Empty,
            "otel.status_description" = ::tracing::field::Empty,
            // Peer service name (if set)
            "peer.service" = $attributes.name,
        )
    };
}

/// Helper macro for executor methods that return a BoxFuture
/// (describe, execute, prepare, prepare_with).
#[doc(hidden)]
#[macro_export]
macro_rules! exec_fut {
    ($span_name:expr, $sql:expr, $attrs:expr, $fut:expr) => {{
        let record_details = $attrs.record_error_details;
        let span = $crate::instrument!($span_name, $sql, $attrs);
        let fut = $fut;
        Box::pin(
            async move {
                fut.await
                    .inspect_err(|e| $crate::span::record_error(e, record_details))
            }
            .instrument(span),
        )
    }};
}

/// Helper macro for fetch_all which records the number of returned rows.
#[doc(hidden)]
#[macro_export]
macro_rules! exec_fut_rows {
    ($sql:expr, $attrs:expr, $fut:expr) => {{
        let record_details = $attrs.record_error_details;
        let span = $crate::instrument!("sqlx.fetch_all", $sql, $attrs);
        let fut = $fut;
        Box::pin(
            async move {
                fut.await
                    .inspect(|res| {
                        ::tracing::Span::current().record("db.response.returned_rows", res.len());
                    })
                    .inspect_err(|e| $crate::span::record_error(e, record_details))
            }
            .instrument(span),
        )
    }};
}

/// Helper macro for fetch_one which records returned_rows = 1.
#[doc(hidden)]
#[macro_export]
macro_rules! exec_fut_one {
    ($sql:expr, $attrs:expr, $fut:expr) => {{
        let record_details = $attrs.record_error_details;
        let span = $crate::instrument!("sqlx.fetch_one", $sql, $attrs);
        let fut = $fut;
        Box::pin(
            async move {
                fut.await
                    .inspect($crate::span::record_one)
                    .inspect_err(|e| $crate::span::record_error(e, record_details))
            }
            .instrument(span),
        )
    }};
}

/// Helper macro for fetch_optional which records returned_rows 0 or 1.
#[doc(hidden)]
#[macro_export]
macro_rules! exec_fut_opt {
    ($sql:expr, $attrs:expr, $fut:expr) => {{
        let record_details = $attrs.record_error_details;
        let span = $crate::instrument!("sqlx.fetch_optional", $sql, $attrs);
        let fut = $fut;
        Box::pin(
            async move {
                fut.await
                    .inspect($crate::span::record_optional)
                    .inspect_err(|e| $crate::span::record_error(e, record_details))
            }
            .instrument(span),
        )
    }};
}

/// Helper macro for stream-based executor methods (execute_many, fetch, fetch_many).
#[doc(hidden)]
#[macro_export]
macro_rules! exec_stream {
    ($span_name:expr, $sql:expr, $attrs:expr, $stream:expr) => {{
        let record_details = $attrs.record_error_details;
        let span = $crate::instrument!($span_name, $sql, $attrs);
        Box::pin(
            $stream
                .inspect(move |_| {
                    let _enter = span.enter();
                })
                .inspect_err(move |e| $crate::span::record_error(e, record_details)),
        )
    }};
}

/// Macro to create a tracing span for a non-SQL lifecycle operation with OpenTelemetry-compatible fields.
///
/// - `$name`: The operation name (e.g., "sqlx.pool.acquire", "sqlx.transaction.commit").
/// - `$attributes`: Connection or pool attributes for peer and db context.
///
/// This macro is used internally for pool and transaction lifecycle operations
/// that don't have an associated SQL statement. It omits query-specific fields
/// like `db.query.text`, `db.sql.table`, and `db.response.*`.
#[doc(hidden)]
#[macro_export]
macro_rules! instrument_op {
    ($name:expr, $attributes:expr) => {
        tracing::info_span!(
            $name,
            // Database name (if available)
            "db.name" = $attributes.database,
            // Database system (e.g., "postgresql", "sqlite")
            "db.system.name" = DB::SYSTEM,
            // Error type, message, and stacktrace (to be filled on error)
            "error.type" = ::tracing::field::Empty,
            "error.message" = ::tracing::field::Empty,
            "error.stacktrace" = ::tracing::field::Empty,
            // Peer (server) host and port
            "net.peer.name" = $attributes.host,
            "net.peer.port" = $attributes.port,
            // OpenTelemetry semantic fields
            "otel.kind" = "client",
            "otel.status_code" = ::tracing::field::Empty,
            "otel.status_description" = ::tracing::field::Empty,
            // Peer service name (if set)
            "peer.service" = $attributes.name,
        )
    };
}

/// Records that a single row was returned in the current tracing span.
/// Used for fetch_one operations.
pub fn record_one<T>(_value: &T) {
    let span = tracing::Span::current();
    span.record("db.response.returned_rows", 1);
}

/// Records whether an optional row was returned in the current tracing span.
/// Used for fetch_optional operations.
pub fn record_optional<T>(value: &Option<T>) {
    let span = tracing::Span::current();
    span.record(
        "db.response.returned_rows",
        if value.is_some() { 1 } else { 0 },
    );
}

/// Records error details in the current tracing span for a SQLx error.
/// Sets OpenTelemetry status and error fields for observability backends.
///
/// When `record_details` is false, only the error type (client/server) and
/// status code are recorded, omitting potentially sensitive error messages
/// and stacktraces.
pub fn record_error(err: &sqlx::Error, record_details: bool) {
    let span = tracing::Span::current();
    // Mark the span as an error for OpenTelemetry
    span.record("otel.status_code", "error");
    // Classify error type as client or server
    match err {
        sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::Decode { .. }
        | sqlx::Error::Encode { .. }
        | sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. } => {
            span.record("error.type", "client");
        }
        _ => {
            span.record("error.type", "server");
        }
    }
    if record_details {
        let msg = err.to_string();
        span.record("otel.status_description", &msg);
        span.record("error.message", msg);
        span.record("error.stacktrace", format!("{err:?}"));
    }
}

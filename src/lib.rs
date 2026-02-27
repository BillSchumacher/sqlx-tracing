#![doc = include_str!("../README.md")]

use std::sync::Arc;

use tracing::Instrument;

mod connection;
mod pool;
pub mod prelude;
pub(crate) mod span;
mod transaction;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

/// Attributes describing the database connection and context.
/// Used for span enrichment and attribute propagation.
#[derive(Debug)]
struct Attributes {
    name: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    record_query_text: bool,
    record_error_details: bool,
}

impl Default for Attributes {
    fn default() -> Self {
        Self {
            name: None,
            host: None,
            port: None,
            database: None,
            record_query_text: true,
            record_error_details: true,
        }
    }
}

/// Builder for constructing a [`Pool`] with custom attributes.
///
/// Allows setting database name, host, port, and other identifying information
/// for tracing purposes.
#[derive(Debug)]
pub struct PoolBuilder<DB: sqlx::Database> {
    pool: sqlx::Pool<DB>,
    attributes: Attributes,
}

// this is required because `pool.connect_options().to_url_lossy()` panics with sqlite
#[cfg(feature = "postgres")]
impl From<sqlx::Pool<sqlx::Postgres>> for PoolBuilder<sqlx::Postgres> {
    /// Create a new builder from an existing SQLx pool.
    fn from(pool: sqlx::Pool<sqlx::Postgres>) -> Self {
        use sqlx::ConnectOptions;

        let url = pool.connect_options().to_url_lossy();
        let attributes = Attributes {
            host: url.host_str().map(String::from),
            port: url.port(),
            database: url
                .path_segments()
                .and_then(|mut segments| segments.next().map(String::from)),
            ..Default::default()
        };
        Self { pool, attributes }
    }
}

// this is required because `pool.connect_options().to_url_lossy()` panics with sqlite
#[cfg(feature = "sqlite")]
impl From<sqlx::Pool<sqlx::Sqlite>> for PoolBuilder<sqlx::Sqlite> {
    /// Create a new builder from an existing SQLx pool.
    fn from(pool: sqlx::Pool<sqlx::Sqlite>) -> Self {
        let attributes = Attributes {
            host: pool
                .connect_options()
                .get_filename()
                .to_str()
                .map(String::from),
            ..Default::default()
        };
        Self { pool, attributes }
    }
}

impl<DB: sqlx::Database> PoolBuilder<DB> {
    /// Set a custom name for the pool (for peer.service attribute).
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.attributes.name = Some(name.into());
        self
    }

    /// Set the database name attribute.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.attributes.database = Some(database.into());
        self
    }

    /// Set the host attribute.
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.attributes.host = Some(host.into());
        self
    }

    /// Set the port attribute.
    pub fn with_port(mut self, port: u16) -> Self {
        self.attributes.port = Some(port);
        self
    }

    /// Enable or disable recording of SQL query text in spans.
    ///
    /// When disabled, the `db.query.text` span field will be empty.
    /// This can be useful in environments where SQL queries may contain
    /// sensitive data that should not flow to the observability backend.
    ///
    /// Enabled by default.
    pub fn with_query_text_recording(mut self, enabled: bool) -> Self {
        self.attributes.record_query_text = enabled;
        self
    }

    /// Enable or disable recording of detailed error information in spans.
    ///
    /// When disabled, error spans will only record the error type
    /// (client/server) and status code, omitting the error message and
    /// Debug-format stacktrace. This can be useful when error messages might
    /// contain sensitive information such as connection strings or internal
    /// database state.
    ///
    /// Enabled by default.
    pub fn with_error_detail_recording(mut self, enabled: bool) -> Self {
        self.attributes.record_error_details = enabled;
        self
    }

    /// Build the [`Pool`] with the configured attributes.
    pub fn build(self) -> Pool<DB> {
        Pool {
            inner: self.pool,
            attributes: Arc::new(self.attributes),
        }
    }
}

/// An asynchronous pool of SQLx database connections with tracing instrumentation.
///
/// Wraps a SQLx [`Pool`] and propagates tracing attributes to all acquired connections.
#[derive(Clone, Debug)]
pub struct Pool<DB>
where
    DB: sqlx::Database,
{
    inner: sqlx::Pool<DB>,
    attributes: Arc<Attributes>,
}

impl<DB> From<sqlx::Pool<DB>> for Pool<DB>
where
    DB: sqlx::Database,
    PoolBuilder<DB>: From<sqlx::Pool<DB>>,
{
    /// Convert a SQLx [`Pool`] into a tracing-instrumented [`Pool`].
    fn from(inner: sqlx::Pool<DB>) -> Self {
        PoolBuilder::from(inner).build()
    }
}

impl<DB> AsRef<sqlx::Pool<DB>> for Pool<DB>
where
    DB: sqlx::Database,
{
    fn as_ref(&self) -> &sqlx::Pool<DB> {
        &self.inner
    }
}

impl<DB> Pool<DB>
where
    DB: sqlx::Database,
{
    /// Returns a reference to the underlying [`sqlx::Pool`].
    ///
    /// This allows bypassing the tracing instrumentation for operations
    /// not yet supported by this crate (e.g. `COPY`, `LISTEN/NOTIFY`,
    /// or other database-specific features).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let pool: sqlx_tracing::Pool<sqlx::Postgres> = /* ... */;
    ///
    /// // Use the raw sqlx pool directly for unsupported operations
    /// let raw: &sqlx::PgPool = pool.inner();
    /// ```
    pub fn inner(&self) -> &sqlx::Pool<DB> {
        &self.inner
    }

    /// Returns the number of connections currently active (including idle).
    pub fn size(&self) -> u32 {
        self.inner.size()
    }

    /// Returns the number of idle connections (not currently in use).
    pub fn num_idle(&self) -> usize {
        self.inner.num_idle()
    }

    /// Returns `true` if [`Pool::close`] has been called on this pool.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<DB> Pool<DB>
where
    DB: sqlx::Database + crate::prelude::Database,
{
    /// Retrieves a connection and immediately begins a new transaction.
    ///
    /// The returned [`Transaction`] is instrumented for tracing.
    pub async fn begin<'c>(&'c self) -> Result<Transaction<'c, DB>, sqlx::Error> {
        let attrs = &self.attributes;
        let record_details = attrs.record_error_details;
        let span = crate::instrument_op!("sqlx.transaction.begin", attrs);
        async {
            self.inner
                .begin()
                .await
                .map(|inner| Transaction {
                    inner,
                    attributes: self.attributes.clone(),
                })
                .inspect_err(|e| crate::span::record_error(e, record_details))
        }
        .instrument(span)
        .await
    }

    /// Acquires a pooled connection, instrumented for tracing.
    pub async fn acquire(&self) -> Result<PoolConnection<DB>, sqlx::Error> {
        let attrs = &self.attributes;
        let record_details = attrs.record_error_details;
        let span = crate::instrument_op!("sqlx.pool.acquire", attrs);
        async {
            self.inner
                .acquire()
                .await
                .map(|inner| PoolConnection {
                    attributes: self.attributes.clone(),
                    inner,
                })
                .inspect_err(|e| crate::span::record_error(e, record_details))
        }
        .instrument(span)
        .await
    }

    /// Attempts to acquire a connection from the pool without waiting.
    ///
    /// Returns `None` immediately if no idle connections are available
    /// and the pool is at its connection limit.
    pub fn try_acquire(&self) -> Option<PoolConnection<DB>> {
        let attrs = &self.attributes;
        let span = crate::instrument_op!("sqlx.pool.acquire", attrs);
        let _enter = span.enter();
        self.inner.try_acquire().map(|inner| PoolConnection {
            attributes: self.attributes.clone(),
            inner,
        })
    }

    /// Ends the use of a connection pool.
    ///
    /// Prevents any new connections and will close all active connections
    /// when they are returned to the pool. Does not resolve until all
    /// connections are closed.
    pub async fn close(&self) {
        let attrs = &self.attributes;
        let span = crate::instrument_op!("sqlx.pool.close", attrs);
        async { self.inner.close().await }.instrument(span).await
    }
}

/// Wrapper for a mutable SQLx connection reference with tracing attributes.
///
/// Used internally for transaction and pool connection executors.
pub struct Connection<'c, DB>
where
    DB: sqlx::Database,
{
    inner: &'c mut DB::Connection,
    attributes: Arc<Attributes>,
}

impl<'c, DB: sqlx::Database> std::fmt::Debug for Connection<'c, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

/// A pooled SQLx connection instrumented for tracing.
///
/// Implements [`sqlx::Executor`] and propagates tracing attributes.
#[derive(Debug)]
pub struct PoolConnection<DB>
where
    DB: sqlx::Database,
{
    inner: sqlx::pool::PoolConnection<DB>,
    attributes: Arc<Attributes>,
}

/// An in-progress database transaction or savepoint, instrumented for tracing.
///
/// Wraps a SQLx [`Transaction`] and propagates tracing attributes.
///
/// A `Transaction` is created via [`Pool::begin`] and can be explicitly
/// committed with [`Transaction::commit`] or rolled back with
/// [`Transaction::rollback`]. If dropped without calling either method,
/// the transaction is automatically rolled back (SQLx default behavior).
///
/// Use [`Transaction::executor`] to obtain a tracing-instrumented executor
/// for running queries within the transaction.
#[derive(Debug)]
pub struct Transaction<'c, DB>
where
    DB: sqlx::Database,
{
    inner: sqlx::Transaction<'c, DB>,
    attributes: Arc<Attributes>,
}

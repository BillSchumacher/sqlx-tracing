use futures::{StreamExt, TryStreamExt};
use sqlx::Error;
use tracing::Instrument;

impl<'c, DB> crate::Transaction<'c, DB>
where
    DB: crate::prelude::Database + sqlx::Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
{
    /// Returns a tracing-instrumented executor for this transaction.
    ///
    /// This allows running queries with full span context and attributes.
    pub fn executor(&mut self) -> crate::Connection<'_, DB> {
        crate::Connection {
            inner: &mut *self.inner,
            attributes: self.attributes.clone(),
        }
    }

    /// Commits this transaction or savepoint.
    ///
    /// This consumes the `Transaction`, sending a `COMMIT` statement to the
    /// database. For a top-level transaction, this releases the underlying
    /// connection back to the pool. For a nested transaction or savepoint,
    /// this only commits the savepoint; the outer transaction (and its
    /// connection) remain active.
    ///
    /// # Errors
    ///
    /// Returns [`sqlx::Error`] if the database fails to commit the
    /// transaction (e.g., due to a constraint violation or connectivity issue).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut tx = pool.begin().await?;
    /// sqlx::query("INSERT INTO users (name) VALUES ($1)")
    ///     .bind("Alice")
    ///     .execute(&mut tx.executor())
    ///     .await?;
    /// tx.commit().await?;
    /// ```
    pub async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    /// Aborts this transaction or savepoint.
    ///
    /// This consumes the `Transaction`, sending a `ROLLBACK` statement to the
    /// database and discarding all changes made within the transaction. For a
    /// top-level transaction, the underlying connection is released back to
    /// the pool. For a nested transaction or savepoint, only the savepoint is
    /// rolled back; the outer transaction (and its connection) remain active.
    ///
    /// Note that dropping a `Transaction` without calling [`commit`](Transaction::commit)
    /// will also roll back automatically. Use this method when you want to
    /// explicitly handle the rollback result.
    ///
    /// # Errors
    ///
    /// Returns [`sqlx::Error`] if the database fails to process the rollback
    /// (e.g., due to a connectivity issue).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut tx = pool.begin().await?;
    /// sqlx::query("INSERT INTO users (name) VALUES ($1)")
    ///     .bind("Alice")
    ///     .execute(&mut tx.executor())
    ///     .await?;
    /// // Discard the insert
    /// tx.rollback().await?;
    /// ```
    pub async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
    }
}

/// Implements `sqlx::Executor` for a mutable reference to a tracing-instrumented transaction.
///
/// Each method creates a tracing span for the SQL operation, attaches relevant attributes,
/// and records errors or row counts as appropriate for observability.
impl<'c, DB> sqlx::Executor<'c> for &'c mut crate::Transaction<'c, DB>
where
    DB: crate::prelude::Database + sqlx::Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
{
    type Database = DB;

    // Transaction's describe needs the future created inside the async block
    // because `(&mut self.inner)` borrows through `self` which is consumed
    // by the async move block.
    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        let attrs = &self.attributes;
        let record_details = attrs.record_error_details;
        let span = crate::instrument!("sqlx.describe", sql, attrs);
        Box::pin(
            async move {
                let fut = (&mut self.inner).describe(sql);
                fut.await
                    .inspect_err(|e| crate::span::record_error(e, record_details))
            }
            .instrument(span),
        )
    }

    fn execute<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::Database>::QueryResult, sqlx::Error>,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_fut!("sqlx.execute", sql, attrs, (&mut self.inner).execute(query))
    }

    fn execute_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<<Self::Database as sqlx::Database>::QueryResult, sqlx::Error>,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_stream!(
            "sqlx.execute_many",
            sql,
            attrs,
            (&mut self.inner).execute_many(query)
        )
    }

    fn fetch<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<'e, Result<<Self::Database as sqlx::Database>::Row, sqlx::Error>>
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_stream!("sqlx.fetch", sql, attrs, (&mut self.inner).fetch(query))
    }

    fn fetch_all<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Vec<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_fut_rows!(sql, attrs, (&mut self.inner).fetch_all(query))
    }

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Self::Database as sqlx::Database>::QueryResult,
                <Self::Database as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_stream!(
            "sqlx.fetch_many",
            sql,
            attrs,
            (&mut self.inner).fetch_many(query)
        )
    }

    fn fetch_one<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<'e, Result<<Self::Database as sqlx::Database>::Row, sqlx::Error>>
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_fut_one!(sql, attrs, (&mut self.inner).fetch_one(query))
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        E: 'q + sqlx::Execute<'q, Self::Database>,
        'c: 'e,
    {
        let sql = query.sql();
        let attrs = &self.attributes;
        crate::exec_fut_opt!(sql, attrs, (&mut self.inner).fetch_optional(query))
    }

    fn prepare<'e, 'q: 'e>(
        self,
        query: &'q str,
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::Database>::Statement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        let attrs = &self.attributes;
        crate::exec_fut!(
            "sqlx.prepare",
            query,
            attrs,
            (&mut self.inner).prepare(query)
        )
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::Database>::Statement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        let attrs = &self.attributes;
        crate::exec_fut!(
            "sqlx.prepare_with",
            sql,
            attrs,
            (&mut self.inner).prepare_with(sql, parameters)
        )
    }
}

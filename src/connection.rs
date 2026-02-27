use futures::{StreamExt, TryStreamExt};
use tracing::Instrument;

impl<DB> AsMut<<DB as sqlx::Database>::Connection> for crate::PoolConnection<DB>
where
    DB: crate::prelude::Database + sqlx::Database,
{
    fn as_mut(&mut self) -> &mut <DB as sqlx::Database>::Connection {
        self.inner.as_mut()
    }
}

impl<DB> crate::PoolConnection<DB>
where
    DB: crate::prelude::Database + sqlx::Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
{
    /// Pings the database to check if the connection is still valid.
    ///
    /// The ping operation is instrumented with a `sqlx.connection.ping` tracing span.
    pub async fn ping(&mut self) -> Result<(), sqlx::Error> {
        use sqlx::Connection;
        let attrs = &self.attributes;
        let record_details = attrs.record_error_details;
        let span = crate::instrument_op!("sqlx.connection.ping", attrs);
        async {
            self.inner
                .as_mut()
                .ping()
                .await
                .inspect_err(|e| crate::span::record_error(e, record_details))
        }
        .instrument(span)
        .await
    }

    /// Begins a new transaction on this connection.
    ///
    /// The returned [`Transaction`](crate::Transaction) is instrumented for tracing.
    pub async fn begin(&mut self) -> Result<crate::Transaction<'_, DB>, sqlx::Error> {
        use sqlx::Connection;
        let attrs = &self.attributes;
        let record_details = attrs.record_error_details;
        let span = crate::instrument_op!("sqlx.transaction.begin", attrs);
        async {
            self.inner
                .as_mut()
                .begin()
                .await
                .map(|inner| crate::Transaction {
                    inner,
                    attributes: self.attributes.clone(),
                })
                .inspect_err(|e| crate::span::record_error(e, record_details))
        }
        .instrument(span)
        .await
    }
}

impl<'c, DB> sqlx::Executor<'c> for &'c mut crate::PoolConnection<DB>
where
    DB: crate::prelude::Database + sqlx::Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
{
    type Database = DB;

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        let attrs = &self.attributes;
        crate::exec_fut!(
            "sqlx.describe",
            sql,
            attrs,
            self.inner.as_mut().describe(sql)
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
        crate::exec_fut!("sqlx.execute", sql, attrs, self.inner.execute(query))
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
            self.inner.execute_many(query)
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
        crate::exec_stream!("sqlx.fetch", sql, attrs, self.inner.fetch(query))
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
        crate::exec_fut_rows!(sql, attrs, self.inner.fetch_all(query))
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
        crate::exec_stream!("sqlx.fetch_many", sql, attrs, self.inner.fetch_many(query))
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
        crate::exec_fut_one!(sql, attrs, self.inner.fetch_one(query))
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
        crate::exec_fut_opt!(sql, attrs, self.inner.fetch_optional(query))
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
        crate::exec_fut!("sqlx.prepare", query, attrs, self.inner.prepare(query))
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
            self.inner.prepare_with(sql, parameters)
        )
    }
}

impl<'c, DB> sqlx::Executor<'c> for &'c mut crate::Connection<'c, DB>
where
    DB: crate::prelude::Database + sqlx::Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
{
    type Database = DB;

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        let attrs = &self.attributes;
        crate::exec_fut!("sqlx.describe", sql, attrs, self.inner.describe(sql))
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
        crate::exec_fut!("sqlx.execute", sql, attrs, self.inner.execute(query))
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
            self.inner.execute_many(query)
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
        crate::exec_stream!("sqlx.fetch", sql, attrs, self.inner.fetch(query))
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
        crate::exec_fut_rows!(sql, attrs, self.inner.fetch_all(query))
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
        crate::exec_stream!("sqlx.fetch_many", sql, attrs, self.inner.fetch_many(query))
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
        crate::exec_fut_one!(sql, attrs, self.inner.fetch_one(query))
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
        crate::exec_fut_opt!(sql, attrs, self.inner.fetch_optional(query))
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
        crate::exec_fut!("sqlx.prepare", query, attrs, self.inner.prepare(query))
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
            self.inner.prepare_with(sql, parameters)
        )
    }
}

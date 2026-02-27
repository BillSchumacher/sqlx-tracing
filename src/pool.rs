use futures::{StreamExt, TryStreamExt};
use tracing::Instrument;

impl<'p, DB> sqlx::Executor<'p> for &'_ crate::Pool<DB>
where
    DB: sqlx::Database + crate::prelude::Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
{
    type Database = DB;

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>> {
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
    > {
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
    > {
        let attrs = &self.attributes;
        crate::exec_fut!(
            "sqlx.prepare_with",
            sql,
            attrs,
            self.inner.prepare_with(sql, parameters)
        )
    }
}

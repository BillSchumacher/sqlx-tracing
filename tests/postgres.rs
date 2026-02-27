#![cfg(feature = "postgres")]

use std::time::Duration;

use sqlx::Postgres;
use testcontainers::{
    GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};

mod common;

#[derive(Debug)]
struct PostgresContainer {
    container: testcontainers::ContainerAsync<testcontainers::GenericImage>,
}

impl PostgresContainer {
    async fn create() -> Self {
        let container = GenericImage::new("postgres", "15-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .with_startup_timeout(Duration::from_secs(60))
            .start()
            .await
            .expect("starting a postgres database");

        Self { container }
    }

    async fn client(&self) -> sqlx_tracing::Pool<Postgres> {
        let port = self.container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres@localhost:{port}/postgres");
        sqlx::PgPool::connect(&url)
            .await
            .map(sqlx_tracing::Pool::from)
            .unwrap()
    }
}

#[tokio::test]
async fn execute() {
    let observability = opentelemetry_testing::ObservabilityContainer::create().await;
    let provider = observability.install().await;

    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    common::should_trace("trace_pool", "postgresql", &observability, &provider, &pool).await;

    {
        let mut conn = pool.acquire().await.unwrap();
        common::should_trace(
            "trace_conn",
            "postgresql",
            &observability,
            &provider,
            &mut conn,
        )
        .await;
    }

    {
        let mut tx: sqlx_tracing::Transaction<'_, Postgres> = pool.begin().await.unwrap();
        common::should_trace(
            "trace_tx",
            "postgresql",
            &observability,
            &provider,
            &mut tx.executor(),
        )
        .await;
    }
}

#[tokio::test]
async fn transaction_commit() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Create a table.
    sqlx::query("CREATE TABLE test_commit (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // Insert a row inside a transaction and commit.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO test_commit (value) VALUES ('hello')")
        .execute(&mut tx.executor())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // The row should be visible after commit.
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_commit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn transaction_rollback() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Create a table.
    sqlx::query("CREATE TABLE test_rollback (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // Insert a row inside a transaction and roll back.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO test_rollback (value) VALUES ('hello')")
        .execute(&mut tx.executor())
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    // The row should NOT be visible after rollback.
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_rollback")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
async fn inner_returns_underlying_pool() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Use inner() to bypass tracing and execute directly on the sqlx pool.
    let inner: &sqlx::PgPool = pool.inner();
    let result: (i32,) = sqlx::query_as("SELECT 1").fetch_one(inner).await.unwrap();
    assert_eq!(result.0, 1);
}

#[tokio::test]
async fn as_ref_returns_underlying_pool() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Use AsRef to bypass tracing and execute directly on the sqlx pool.
    let inner: &sqlx::PgPool = pool.as_ref();
    let result: (i32,) = sqlx::query_as("SELECT 1").fetch_one(inner).await.unwrap();
    assert_eq!(result.0, 1);
}

#[tokio::test]
async fn inner_and_traced_share_same_pool() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Create a table via the inner (untraced) pool.
    sqlx::query("CREATE TABLE test_inner (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        .execute(pool.inner())
        .await
        .unwrap();

    // Insert via the inner pool.
    sqlx::query("INSERT INTO test_inner (value) VALUES ('from_inner')")
        .execute(pool.inner())
        .await
        .unwrap();

    // Read via the traced pool -- should see the row inserted through inner.
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_inner")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn pool_stats() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    assert!(!pool.is_closed());

    // Acquire a connection to ensure at least one active connection.
    let conn = pool.acquire().await.unwrap();
    assert!(pool.size() >= 1);

    // After dropping, it should return to idle.
    drop(conn);
    assert!(pool.size() >= 1);
}

#[tokio::test]
async fn try_acquire_returns_connection() {
    let container = PostgresContainer::create().await;

    // Create a pool with max 1 connection and min 1 to pre-create it.
    let port = container.container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres@localhost:{port}/postgres");
    let pool = sqlx::pool::PoolOptions::<Postgres>::new()
        .max_connections(1)
        .min_connections(1)
        .connect(&url)
        .await
        .map(sqlx_tracing::Pool::from)
        .unwrap();

    // Give the pool a moment to establish its min_connections.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // try_acquire should succeed when a connection is idle.
    let conn = pool.try_acquire();
    assert!(conn.is_some());

    // With the only connection checked out, try_acquire should return None.
    let second = pool.try_acquire();
    assert!(second.is_none());
}

#[tokio::test]
async fn pool_close() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    assert!(!pool.is_closed());
    pool.close().await;
    assert!(pool.is_closed());
}

#[tokio::test]
async fn connection_ping() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    let mut conn = pool.acquire().await.unwrap();
    // Ping should succeed on a healthy connection.
    conn.ping().await.unwrap();
}

#[tokio::test]
async fn connection_begin_transaction() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Create a table.
    sqlx::query("CREATE TABLE test_conn_begin (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // Begin a transaction from a connection (not from the pool).
    let mut conn = pool.acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("INSERT INTO test_conn_begin (value) VALUES ('from_conn')")
        .execute(&mut tx.executor())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(conn);

    // Verify the row was committed.
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_conn_begin")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn transaction_drop_rolls_back() {
    let container = PostgresContainer::create().await;
    let pool = container.client().await;

    // Create a table.
    sqlx::query("CREATE TABLE test_drop (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // Insert a row inside a transaction and drop without commit.
    {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("INSERT INTO test_drop (value) VALUES ('hello')")
            .execute(&mut tx.executor())
            .await
            .unwrap();
        // tx is dropped here without commit or rollback
    }

    // The row should NOT be visible (implicit rollback on drop).
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_drop")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

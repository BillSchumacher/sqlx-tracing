#![cfg(feature = "sqlite")]

use sqlx::Sqlite;

mod common;

#[tokio::test]
async fn execute() {
    let observability = opentelemetry_testing::ObservabilityContainer::create().await;
    let provider = observability.install().await;

    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    common::should_trace("trace_pool", "sqlite", &observability, &provider, &pool).await;

    {
        let mut conn = pool.acquire().await.unwrap();
        common::should_trace("trace_conn", "sqlite", &observability, &provider, &mut conn).await;
    }

    {
        let mut tx: sqlx_tracing::Transaction<'_, Sqlite> = pool.begin().await.unwrap();
        common::should_trace(
            "trace_tx",
            "sqlite",
            &observability,
            &provider,
            &mut tx.executor(),
        )
        .await;
    }
}

#[tokio::test]
async fn transaction_commit() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Create a table.
    sqlx::query("CREATE TABLE test_commit (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
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
    let count: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM test_commit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn transaction_rollback() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Create a table.
    sqlx::query("CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
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
    let count: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM test_rollback")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
async fn inner_returns_underlying_pool() {
    let raw_pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    let pool = sqlx_tracing::Pool::from(raw_pool);

    // Use inner() to bypass tracing and execute directly on the sqlx pool.
    let inner: &sqlx::SqlitePool = pool.inner();
    let result: (i32,) = sqlx::query_as("SELECT 1").fetch_one(inner).await.unwrap();
    assert_eq!(result.0, 1);
}

#[tokio::test]
async fn as_ref_returns_underlying_pool() {
    let raw_pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    let pool = sqlx_tracing::Pool::from(raw_pool);

    // Use AsRef to bypass tracing and execute directly on the sqlx pool.
    let inner: &sqlx::SqlitePool = pool.as_ref();
    let result: (i32,) = sqlx::query_as("SELECT 1").fetch_one(inner).await.unwrap();
    assert_eq!(result.0, 1);
}

#[tokio::test]
async fn inner_and_traced_share_same_pool() {
    let raw_pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(raw_pool);

    // Create a table via the inner (untraced) pool.
    sqlx::query("CREATE TABLE test_inner (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .execute(pool.inner())
        .await
        .unwrap();

    // Insert via the inner pool.
    sqlx::query("INSERT INTO test_inner (value) VALUES ('from_inner')")
        .execute(pool.inner())
        .await
        .unwrap();

    // Read via the traced pool -- should see the row inserted through inner.
    let count: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM test_inner")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn pool_stats() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(5)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

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
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .min_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Give the pool a moment to establish its min_connections.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // try_acquire should succeed when a connection is idle.
    let conn = pool.try_acquire();
    assert!(conn.is_some());

    // With the only connection checked out, try_acquire should return None.
    let second = pool.try_acquire();
    assert!(second.is_none());
}

#[tokio::test]
async fn pool_close() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    assert!(!pool.is_closed());
    pool.close().await;
    assert!(pool.is_closed());
}

#[tokio::test]
async fn connection_ping() {
    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    let mut conn = pool.acquire().await.unwrap();
    // Ping should succeed on a healthy connection.
    conn.ping().await.unwrap();
}

#[tokio::test]
async fn connection_begin_transaction() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Create a table.
    sqlx::query("CREATE TABLE test_conn_begin (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
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
    let count: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM test_conn_begin")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn transaction_drop_rolls_back() {
    let pool = sqlx::pool::PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Create a table.
    sqlx::query("CREATE TABLE test_drop (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
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
    let count: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM test_drop")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

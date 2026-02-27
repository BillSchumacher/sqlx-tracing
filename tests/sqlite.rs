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
    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    let pool = sqlx_tracing::Pool::from(pool);

    // Create a table using a connection so it persists in the pool.
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
    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
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
async fn transaction_drop_rolls_back() {
    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
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

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0](https://github.com/BillSchumacher/sqlx-tracing/compare/v0.2.0...v0.3.0) - 2026-02-27

### Other

- Fix try_acquire_returns_connection test timing issue in postgres
- Update documentation and add tests for pool lifecycle tracing
- Add tracing spans for pool lifecycle and transaction operations
- Add documentation and tests for Pool::inner() and AsRef
- Expose underlying sqlx::Pool via inner() and AsRef
- Implement security and performance audit recommendations
- Fix security and performance issues found during audit
- Revert workflow branch names back to main
- Fix CI workflows to use master branch instead of main
- Address review: savepoint docs and SQLite pool isolation
- Add documentation and tests for Transaction::commit and Transaction::rollback
- add `Transaction::commit` + `Transaction::rollback`

### Added

- expose underlying `sqlx::Pool` via `Pool::inner()` method and `AsRef<sqlx::Pool<DB>>` impl
- trace `Pool::acquire()` with `sqlx.pool.acquire` span for connection acquisition latency
- trace `Pool::begin()` with `sqlx.transaction.begin` span for transaction initiation
- trace `Transaction::commit()` with `sqlx.transaction.commit` span
- trace `Transaction::rollback()` with `sqlx.transaction.rollback` span
- add `Pool::close()` with `sqlx.pool.close` span for graceful shutdown observability
- add `Pool::try_acquire()` with tracing for non-blocking pool acquisition
- add `Pool::size()`, `Pool::num_idle()`, `Pool::is_closed()` for pool health metrics
- add `PoolConnection::ping()` with `sqlx.connection.ping` span for health checks
- add `PoolConnection::begin()` with `sqlx.transaction.begin` span to start transactions from connections
- add `instrument_op!` macro for lifecycle operation spans (lighter than `instrument!` for non-SQL operations)

## [0.2.0](https://github.com/jdrouet/sqlx-tracing/compare/v0.1.0...v0.2.0) - 2025-10-02

### Added

- add attributes to pool
- make sure returned_rows is populated
- trace on pool connections and transactions
- make it work with PoolConnection
- make transaction part compile
- create pool-connection and transaction

### Fixed

- unused import
- create separate builder for sqlite and postgres
- please clippy
- remove unused traits

### Other

- use opentelemetry-testing from registry
- comment the code
- update readme with pool builder
- ensure pool queries are traced
- release v0.1.0

## [0.1.0](https://github.com/jdrouet/sqlx-tracing/releases/tag/v0.1.0) - 2025-09-07

### Other

- configure for auto release
- update cargo.toml
- set versions in dev deps
- add readme
- configure
- check that it works for sqlite and postgres
- simple project

use crate::sqllog::Sqllog;
use anyhow::{Context, Result};
use duckdb::{Connection, ToSql, appender_params_from_iter, params};
use log::{debug, info, warn};
use serde::Serialize;
use std::env;
use std::path::Path;
use std::time::Instant;

// Type alias placed at module level to avoid declaring items after statements
// (clippy::items_after_statements). This alias represents one row prepared for
// the DuckDB appender and reduces visual type complexity in the function body.
type AppenderRow = (
    String,
    i32,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

// No helper needed: `Sqllog` stores i64 fields now.

/// Default entry: uses chunk size `1000`.
///
/// # Errors
/// Returns an error if opening the database, creating the table, or writing fails.
pub fn write_sqllogs_to_duckdb<P: AsRef<Path>>(db_path: P, records: &[Sqllog]) -> Result<()> {
    // default behavior: use chunk 1000, env var controls index creation
    write_sqllogs_to_duckdb_impl(db_path, records, 1000, None).map(|_| ())
}

/// Write records into `DuckDB` using the Appender API with configurable `chunk_size`.
/// `chunk_size == 0` is normalized to `1`.
///
/// # Errors
/// Returns an error if opening the database, creating the table, or writing fails.
#[allow(clippy::too_many_lines)]
pub fn write_sqllogs_to_duckdb_with_chunk<P: AsRef<Path>>(
    db_path: P,
    records: &[Sqllog],
    chunk_size: usize,
) -> Result<()> {
    // default: honor environment variable if set
    write_sqllogs_to_duckdb_impl(db_path, records, chunk_size, None).map(|_| ())
}

/// Public API to explicitly control whether indexes are created after writing.
/// Returns a Vec of (`index_statement`, `elapsed_ms`) for created indexes.
/// # Errors
/// Returns an error if opening the database, creating the table, appender usage,
/// or index creation fails.
/// Reports the result of attempting to create an index.
#[derive(Debug, Serialize)]
pub struct IndexReport {
    /// The CREATE INDEX statement executed.
    pub statement: String,
    /// Elapsed time in milliseconds when the statement succeeded.
    pub elapsed_ms: Option<u128>,
    /// If creation failed, this contains the error string.
    pub error: Option<String>,
}

/// Public API to explicitly control whether indexes are created after writing.
/// Returns a Vec of `IndexReport` describing success/failure for each attempted index.
/// # Errors
/// Returns an error if opening the database, creating the table, or appender usage fails.
pub fn write_sqllogs_to_duckdb_with_chunk_and_report<P: AsRef<Path>>(
    db_path: P,
    records: &[Sqllog],
    chunk_size: usize,
    create_indexes: bool,
) -> Result<Vec<IndexReport>> {
    Ok(
        write_sqllogs_to_duckdb_impl(db_path, records, chunk_size, Some(create_indexes))?
            .unwrap_or_default(),
    )
}

// Core implementation shared by public wrappers. If `create_indexes_override` is
// Some(true/false) it overrides the environment variable; if None the env var
// SQLOG_CREATE_INDEXES determines behavior (default true).
#[allow(clippy::too_many_lines)]
fn write_sqllogs_to_duckdb_impl<P: AsRef<Path>>(
    db_path: P,
    records: &[Sqllog],
    chunk_size: usize,
    create_indexes_override: Option<bool>,
) -> Result<Option<Vec<IndexReport>>> {
    let mut conn = Connection::open(db_path.as_ref()).with_context(|| {
        format!(
            "failed to open duckdb database {}",
            db_path.as_ref().display()
        )
    })?;

    let create_sql = r"CREATE TABLE IF NOT EXISTS sqllogs (
        occurrence_time TEXT NOT NULL,
        ep INTEGER NOT NULL,
        session TEXT,
        thread TEXT,
        user TEXT,
        trx_id TEXT,
        statement TEXT,
        appname TEXT,
        ip TEXT,
        sql_type TEXT,
        description TEXT NOT NULL,
        execute_time BIGINT,
        rowcount BIGINT,
        execute_id BIGINT
    )";

    conn.execute(create_sql, params![])?;

    let tx = conn.transaction()?;
    let mut app = tx.appender("sqllogs")?;

    let chunk_size = if chunk_size == 0 { 1 } else { chunk_size };

    let mut chunk_owned: Vec<AppenderRow> = Vec::with_capacity(chunk_size);

    for r in records {
        let execute_time_i = r.execute_time;
        let rowcount_i = r.rowcount;
        let execute_id_i = r.execute_id;

        chunk_owned.push((
            r.occurrence_time.clone(),
            r.ep,
            r.session.clone(),
            r.thread.clone(),
            r.user.clone(),
            r.trx_id.clone(),
            r.statement.clone(),
            r.appname.clone(),
            r.ip.clone(),
            r.sql_type.clone(),
            r.description.clone(),
            execute_time_i,
            rowcount_i,
            execute_id_i,
        ));

        if chunk_owned.len() >= chunk_size {
            let drained = std::mem::take(&mut chunk_owned);
            let rows = drained.into_iter().map(|t| {
                appender_params_from_iter(vec![
                    Box::new(t.0) as Box<dyn ToSql>,
                    Box::new(t.1) as Box<dyn ToSql>,
                    Box::new(t.2) as Box<dyn ToSql>,
                    Box::new(t.3) as Box<dyn ToSql>,
                    Box::new(t.4) as Box<dyn ToSql>,
                    Box::new(t.5) as Box<dyn ToSql>,
                    Box::new(t.6) as Box<dyn ToSql>,
                    Box::new(t.7) as Box<dyn ToSql>,
                    Box::new(t.8) as Box<dyn ToSql>,
                    Box::new(t.9) as Box<dyn ToSql>,
                    Box::new(t.10) as Box<dyn ToSql>,
                    Box::new(t.11) as Box<dyn ToSql>,
                    Box::new(t.12) as Box<dyn ToSql>,
                    Box::new(t.13) as Box<dyn ToSql>,
                ])
            });

            app.append_rows(rows)?;
            app.flush()?;
            chunk_owned = Vec::with_capacity(chunk_size);
        }
    }

    if !chunk_owned.is_empty() {
        let drained = std::mem::take(&mut chunk_owned);
        let rows = drained.into_iter().map(|t| {
            appender_params_from_iter(vec![
                Box::new(t.0) as Box<dyn ToSql>,
                Box::new(t.1) as Box<dyn ToSql>,
                Box::new(t.2) as Box<dyn ToSql>,
                Box::new(t.3) as Box<dyn ToSql>,
                Box::new(t.4) as Box<dyn ToSql>,
                Box::new(t.5) as Box<dyn ToSql>,
                Box::new(t.6) as Box<dyn ToSql>,
                Box::new(t.7) as Box<dyn ToSql>,
                Box::new(t.8) as Box<dyn ToSql>,
                Box::new(t.9) as Box<dyn ToSql>,
                Box::new(t.10) as Box<dyn ToSql>,
                Box::new(t.11) as Box<dyn ToSql>,
                Box::new(t.12) as Box<dyn ToSql>,
                Box::new(t.13) as Box<dyn ToSql>,
            ])
        });

        app.append_rows(rows)?;
        app.flush()?;
    }

    drop(app);
    tx.commit()?;

    // Create indexes after bulk insert to speed up subsequent queries.
    // Use IF NOT EXISTS to make this idempotent. We run each index creation in
    // its own short-lived transaction so a failing index creation doesn't leave
    // the database in a partial state and so we can capture per-index errors.
    // build the list of index statements; allow test injection of a bad
    // statement via SQLOG_INJECT_BAD_INDEX to exercise failure handling.
    let mut index_statements: Vec<String> = vec![
        "CREATE INDEX IF NOT EXISTS idx_sqllogs_trx_id ON sqllogs(trx_id)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_sqllogs_thread ON sqllogs(thread)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_sqllogs_session ON sqllogs(session)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_sqllogs_ip ON sqllogs(ip)".to_string(),
    ];

    if env::var("SQLOG_INJECT_BAD_INDEX").is_ok() {
        // inject a statement that should fail (nonexistent column) to test
        // error reporting. Tests can set SQLOG_INJECT_BAD_INDEX=1 temporarily.
        index_statements
            .push("CREATE INDEX idx_sqllogs_bad ON sqllogs(nonexistent_column)".to_string());
    }

    // decide whether to create indexes:
    let create_indexes = create_indexes_override.map_or_else(
        || {
            env::var("SQLOG_CREATE_INDEXES")
                .map(|v| v != "0")
                .unwrap_or(true)
        },
        |b| b,
    );

    if !create_indexes {
        info!("SQLOG_CREATE_INDEXES=0 -> skipping index creation");
        return Ok(None);
    }

    // log level for index creation messages (info or debug). Default: info.
    let index_log_level = env::var("SQLOG_INDEX_LOG_LEVEL").unwrap_or_else(|_| "info".into());
    let use_debug = index_log_level.eq_ignore_ascii_case("debug");

    let mut reports: Vec<IndexReport> = Vec::with_capacity(index_statements.len());
    for stmt in index_statements {
        if use_debug {
            debug!("creating index: {stmt}");
        } else {
            info!("creating index: {stmt}");
        }

        // each index in its own transaction
        let start = Instant::now();
        let idx_result = (|| -> Result<Option<u128>> {
            let tx = conn.transaction()?;
            // executing within this transaction; stmt is owned String so pass &str
            tx.execute(stmt.as_str(), params![])?;
            tx.commit()?;
            Ok(Some(start.elapsed().as_millis()))
        })();

        match idx_result {
            Ok(Some(ms)) => {
                if use_debug {
                    debug!("created index: {stmt} in {ms} ms");
                } else {
                    info!("created index: {stmt} in {ms} ms");
                }
                reports.push(IndexReport {
                    statement: stmt.clone(),
                    elapsed_ms: Some(ms),
                    error: None,
                });
            }
            Ok(None) => {
                // unexpected empty result
                warn!("index creation returned no timing for: {stmt}");
                reports.push(IndexReport {
                    statement: stmt.clone(),
                    elapsed_ms: None,
                    error: None,
                });
            }
            Err(e) => {
                let err = format!("{e}");
                warn!("failed to create index: {stmt} -> {err}");
                reports.push(IndexReport {
                    statement: stmt.clone(),
                    elapsed_ms: None,
                    error: Some(err),
                });
            }
        }
    }

    Ok(Some(reports))
}

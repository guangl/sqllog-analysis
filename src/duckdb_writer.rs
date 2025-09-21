use crate::config::ExportOptions;
use crate::sqllog::Sqllog;
use anyhow::{Context, Result};
use duckdb::{Connection, ToSql, appender_params_from_iter, params};
use std::path::Path;
use std::time::Instant;
use tracing::info;

const CREATE_TABLE_SQL: &str = r"CREATE TABLE IF NOT EXISTS sqllogs (
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

fn ensure_table(conn: &Connection) -> Result<()> {
    conn.execute(CREATE_TABLE_SQL, params![])?;
    Ok(())
}

fn append_sqllogs(
    app: &mut duckdb::Appender,
    records: &[Sqllog],
) -> Result<()> {
    let rows_iter = records.iter().map(|r| {
        let params: Vec<Box<dyn ToSql>> = vec![
            Box::new(r.occurrence_time.clone()) as Box<dyn ToSql>,
            Box::new(r.ep) as Box<dyn ToSql>,
            Box::new(r.session.clone()) as Box<dyn ToSql>,
            Box::new(r.thread.clone()) as Box<dyn ToSql>,
            Box::new(r.user.clone()) as Box<dyn ToSql>,
            Box::new(r.trx_id.clone()) as Box<dyn ToSql>,
            Box::new(r.statement.clone()) as Box<dyn ToSql>,
            Box::new(r.appname.clone()) as Box<dyn ToSql>,
            Box::new(r.ip.clone()) as Box<dyn ToSql>,
            Box::new(r.sql_type.clone()) as Box<dyn ToSql>,
            Box::new(r.description.clone()) as Box<dyn ToSql>,
            Box::new(r.execute_time) as Box<dyn ToSql>,
            Box::new(r.rowcount) as Box<dyn ToSql>,
            Box::new(r.execute_id) as Box<dyn ToSql>,
        ];
        appender_params_from_iter(params)
    });

    app.append_rows(rows_iter)?;
    Ok(())
}

/// 将一批 `Sqllog` 追加到指定的 `DuckDB` 数据库（使用 `DuckDB` Appender 批量追加）。
///
/// 该函数会确保 `sqllogs` 表存在，然后使用 appender 将所有记录追加并提交事务。
///
/// # Errors
///
/// 当无法打开数据库、确保表或插入/提交失败时会返回 `Err(anyhow::Error)`。调用者应当处理这些错误并在必要时记录或重试。
pub fn write_sqllogs_to_duckdb<P: AsRef<Path>>(
    db_path: P,
    records: &[Sqllog],
) -> Result<()> {
    let mut conn = Connection::open(db_path.as_ref()).with_context(|| {
        format!("打开 DuckDB 数据库失败 {}", db_path.as_ref().display())
    })?;

    ensure_table(&conn)?;

    let tx = conn.transaction()?;
    let mut app = tx.appender("sqllogs")?;

    let db_display = db_path.as_ref().display().to_string();
    let overall_start = Instant::now();

    append_sqllogs(&mut app, records)?;
    app.flush()?;
    let total_written = records.len();

    drop(app);
    tx.commit()?;

    let overall_elapsed = overall_start.elapsed();
    info!(db = %db_display, total_rows = total_written, total_elapsed_ms = overall_elapsed.as_millis(), "已将所有 sqllogs 追加到 DuckDB");

    Ok(())
}

/// 使用 `DuckDB` 的 COPY 命令将 `sqllogs` 表导出到指定文件。
///
/// `format` 支持值："csv"、"json"、"excel"（或 "xlsx"）。
/// 示例：
/// `export_sqllogs_to_file("sqllogs.duckdb", "out.csv", "csv")`。
///
/// # Errors
///
/// 当无法打开数据库或执行 COPY 语句时会返回 `Err(anyhow::Error)`。
/// 调用者应当处理并记录这些错误。
pub fn export_sqllogs_to_file_with_flags<P: AsRef<Path>, Q: AsRef<Path>>(
    db_path: P,
    out_path: Q,
    format: &str,
    options: &ExportOptions,
) -> Result<()> {
    let conn = Connection::open(db_path.as_ref()).with_context(|| {
        format!("打开 DuckDB 数据库失败 {}", db_path.as_ref().display())
    })?;

    // normalize format token
    let fmt_lc = format.to_ascii_lowercase();
    let format_token = match fmt_lc.as_str() {
        "csv" => "CSV",
        "json" => "JSON",
        "excel" | "xlsx" => "XLSX",
        other => {
            return Err(anyhow::anyhow!(
                "不支持的导出格式: {other}, 支持的格式: csv,json,excel"
            ));
        }
    };

    // build COPY option list
    let mut opts: Vec<String> = Vec::new();
    opts.push(format!("FORMAT {format_token}"));
    if fmt_lc == "csv" {
        opts.push("HEADER TRUE".to_string());
    }

    // include the provided flags as additional COPY options only when non-default
    if options.per_thread_out {
        // DuckDB expects PER_THREAD_OUTPUT
        opts.push("PER_THREAD_OUTPUT TRUE".to_string());
    }
    // overwrite_or_ignore is now a bool: include only when true
    if options.write_flags.overwrite_or_ignore {
        opts.push("OVERWRITE_OR_IGNORE TRUE".to_string());
    }
    if options.write_flags.overwrite {
        opts.push("OVERWRITE TRUE".to_string());
    }
    if options.write_flags.append {
        opts.push("APPEND TRUE".to_string());
    }
    if let Some(sz) = options.file_size_bytes {
        opts.push(format!("FILE_SIZE_BYTES {sz}"));
    }

    let options_str = format!("({})", opts.join(", "));

    // escape single quotes in path
    let out_display = out_path.as_ref().to_string_lossy().replace('\'', "''");

    let sql = format!(
        "COPY (SELECT * FROM sqllogs) TO '{out_display}' {options_str}"
    );
    conn.execute(&sql, params![])?;
    info!(db = %db_path.as_ref().display(), to = %out_display, fmt = %format_token, "已通过 COPY 导出 sqllogs");
    Ok(())
}

/// 向后兼容的导出函数（原始签名），使用默认的 flags 值。
///
/// # Errors
///
/// 当实际导出失败时会返回 `Err(anyhow::Error)`。
pub fn export_sqllogs_to_file<P: AsRef<Path>, Q: AsRef<Path>>(
    db_path: P,
    out_path: Q,
    format: &str,
) -> Result<()> {
    // 默认值：不按线程输出；overwrite_or_ignore = "error"；不强制 overwrite；不 append；不限制大小
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: crate::config::WriteFlags {
            overwrite_or_ignore: false,
            overwrite: false,
            append: false,
        },
        file_size_bytes: None,
    };

    export_sqllogs_to_file_with_flags(db_path, out_path, format, &opts)
}

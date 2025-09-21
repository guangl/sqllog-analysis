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

/// 将一批 `Sqllog` 追加到指定的 `DuckDB` 数据库（使用 DuckDB Appender 批量追加）。
///
/// 本函数会确保 `sqllogs` 表存在，然后使用 appender 将所有记录追加并提交事务。
///
/// 错误处理：当无法打开数据库、确保表或插入/提交失败时会返回 `Err(anyhow::Error)`，
/// 调用方应据此记录或重试。
///
/// 写入路径说明：
/// - 当 `use_in_memory` 为 false（默认）时：直接打开磁盘数据库并使用 Appender 批量追加。
/// - 当 `use_in_memory` 为 true 时：在内存 DuckDB 中创建 `sqllogs` 表并使用 Appender 插入所有记录。
///   当前实现的设计决策是：内存路径仅在内存写入（不自动导出到磁盘），以便用于临时分析或快速查询。
///
/// Dispatcher：根据 `use_in_memory` 决定使用哪种写入路径。
pub fn write_sqllogs_to_duckdb<P: AsRef<Path>>(
    db_path: P,
    records: &[Sqllog],
    use_in_memory: bool,
) -> Result<()> {
    if use_in_memory {
        write_sqllogs_via_in_memory(records)
    } else {
        write_sqllogs_direct(db_path, records)
    }
}

/// 直接将记录通过 Appender 写入目标（磁盘）数据库。
fn write_sqllogs_direct<P: AsRef<Path>>(
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

/// 在内存数据库中写入记录。
///
/// 注意：当前实现不会把内存数据库自动 ATTACH 到磁盘或 CTAS 导出到磁盘；内存路径仅把数据保存在内存中。
/// 如果需要把内存数据写回磁盘，请使用持久化的 DuckDB 文件路径进行写入，或自行在调用方层面完成 ATTACH/CTAS 操作。
fn write_sqllogs_via_in_memory(records: &[Sqllog]) -> Result<()> {
    // 使用 ":memory:" 打开内存数据库
    let mut mem_conn =
        Connection::open(":memory:").with_context(|| "打开内存 DuckDB 失败")?;

    ensure_table(&mem_conn)?;

    let tx = mem_conn.transaction()?;
    let mut app = tx.appender("sqllogs")?;

    let overall_start = Instant::now();
    append_sqllogs(&mut app, records)?;
    app.flush()?;
    drop(app);
    tx.commit()?;

    // 如果调用方选择使用内存导出（ == true），
    // 现在的行为是仅在内存 DuckDB 中写入数据并返回；不再把内存 DB 导出到磁盘。
    //
    // 这样便于在内存中进行快速分析或临时查询，而不会在磁盘上创建/覆盖文件。
    let overall_elapsed = overall_start.elapsed();
    info!(
        total_rows = records.len(),
        total_elapsed_ms = overall_elapsed.as_millis(),
        "已在内存 DuckDB 中写入 sqllogs（未导出到磁盘）"
    );
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
    // If caller passed the special in-memory path, we cannot open the
    // same in-memory database here (Connection::open(":memory:") would
    // create a fresh empty in-memory DB). Exporting from an in-memory
    // database requires keeping the original Connection alive or
    // explicitly attaching the in-memory DB to a disk database.
    // Return a helpful error instead of silently operating on a new
    // (empty) disk DB path.
    if db_path.as_ref().to_string_lossy() == ":memory:" {
        return Err(anyhow::anyhow!(
            "无法从匿名内存数据库导出：传入的 db_path=\":memory:\"。\n请使用持久的 DuckDB 文件路径，或在调用导出之前把内存数据库 ATTACH 到磁盘并执行导出，或扩展导出接口以接受一个现存的 Connection。"
        ));
    }

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
        if sz == 0 {
            // DuckDB treats 0 as invalid for FILE_SIZE_BYTES; skip and warn
            tracing::warn!("忽略无效的 file_size_bytes=0；必须为正整数");
        } else {
            opts.push(format!("FILE_SIZE_BYTES {sz}"));
        }
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

/// 创建并返回一个已初始化的内存 DuckDB 连接（已确保 sqllogs 表存在）。
pub fn create_in_memory_connection() -> Result<Connection> {
    let mut conn =
        Connection::open(":memory:").with_context(|| "打开内存 DuckDB 失败")?;
    ensure_table(&conn)?;
    Ok(conn)
}

/// 将记录写入已有的 DuckDB Connection（可用于内存或磁盘连接）。
pub fn write_sqllogs_to_connection(
    conn: &mut Connection,
    records: &[Sqllog],
) -> Result<()> {
    ensure_table(conn)?;
    let tx = conn.transaction()?;
    let mut app = tx.appender("sqllogs")?;
    append_sqllogs(&mut app, records)?;
    app.flush()?;
    drop(app);
    tx.commit()?;
    Ok(())
}

/// 从给定的 Connection 导出 sqllogs 表到指定文件（接受 Connection 而非路径）。
/// 这允许从内存 Connection 导出到磁盘路径。
pub fn export_sqllogs_from_connection<Q: AsRef<Path>>(
    conn: &Connection,
    out_path: Q,
    format: &str,
    options: &ExportOptions,
) -> Result<()> {
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

    if options.per_thread_out {
        opts.push("PER_THREAD_OUTPUT TRUE".to_string());
    }
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
        if sz == 0 {
            tracing::warn!("忽略无效的 file_size_bytes=0；必须为正整数");
        } else {
            opts.push(format!("FILE_SIZE_BYTES {sz}"));
        }
    }

    let options_str = format!("({})", opts.join(", "));
    let out_display = out_path.as_ref().to_string_lossy().replace('\'', "''");
    let sql = format!(
        "COPY (SELECT * FROM sqllogs) TO '{out_display}' {options_str}"
    );
    conn.execute(&sql, params![])?;
    info!(to = %out_display, fmt = %format_token, "已通过 COPY 导出 sqllogs (from Connection)");
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

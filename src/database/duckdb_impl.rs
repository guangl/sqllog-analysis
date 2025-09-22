// DuckDB 数据库提供者实现
//
// 提供 DuckDB 数据库的完整实现，包括：
// - 内存和磁盘模式支持
// - 表结构自动创建
// - 批量数据插入
// - 多格式数据导出
// - 性能优化的查询

use super::{
    DatabaseInfo, DatabaseMode, DatabaseProvider, DatabaseStats, DatabaseType,
    ExportFormat,
};
use crate::config::RuntimeConfig;
use crate::error_writer::ErrorWriter;
use crate::sqllog::Sqllog;
use anyhow::{Context, Result};
use duckdb::{Connection, Result as DuckResult};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

// 类型别名，用于简化复杂的元组类型
type SqllogRowData = (
    String,         // occurrence_time
    String,         // ep
    Option<String>, // session
    Option<String>, // thread
    Option<String>, // username
    Option<String>, // trx_id
    Option<String>, // statement
    Option<String>, // appname
    Option<String>, // ip
    Option<String>, // sql_type
    String,         // description
    Option<i64>,    // execute_time
    Option<i64>,    // rowcount
    Option<i64>,    // execute_id
);

/// `DuckDB` 数据库提供者
///
/// 实现 `DatabaseProvider` trait，提供 `DuckDB` 特定的功能，
/// 包括独立数据库处理能力
pub struct DuckDbProvider {
    /// 数据库连接
    connection: Connection,
    /// 数据库模式
    mode: DatabaseMode,
    /// 是否已初始化
    initialized: bool,
    /// 操作统计信息
    stats: DatabaseStats,
    /// 独立数据库处理统计信息（可选）
    independent_stats: Option<Arc<RwLock<IndependentDatabaseStats>>>,
    /// 线程计数器（用于独立数据库处理）
    thread_counter: Option<Arc<AtomicUsize>>,
}

impl DuckDbProvider {
    /// 创建新的 `DuckDB` 提供者
    ///
    /// # Errors
    /// 当数据库连接创建失败时返回错误
    pub fn new(config: &RuntimeConfig) -> Result<Self> {
        let (connection, mode) = if config.use_in_memory {
            let conn = Connection::open_in_memory()
                .context("无法创建内存数据库连接")?;
            (conn, DatabaseMode::InMemory)
        } else {
            // 确保数据库目录存在
            if let Some(parent) = Path::new(&config.db_path).parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("无法创建数据库目录: {}", parent.display())
                })?;
            }

            let conn =
                Connection::open(&config.db_path).with_context(|| {
                    format!("无法打开数据库文件: {}", config.db_path)
                })?;
            (conn, DatabaseMode::Disk { path: config.db_path.clone() })
        };

        Ok(Self {
            connection,
            mode,
            initialized: false,
            stats: DatabaseStats::default(),
            independent_stats: None,
            thread_counter: None,
        })
    }

    /// 创建 sqllogs 表
    fn create_table(&self) -> DuckResult<()> {
        let create_sql = r"
            CREATE TABLE IF NOT EXISTS sqllogs (
                occurrence_time CHAR(32) NOT NULL,
                ep CHAR(1),
                session VARCHAR(64),
                thread VARCHAR(64),
                username VARCHAR(128),
                trx_id VARCHAR(64),
                statement VARCHAR(64),
                appname VARCHAR(256),
                ip VARCHAR(45),
                sql_type VARCHAR(32),
                description TEXT,
                execute_time BIGINT,
                rowcount BIGINT,
                execute_id BIGINT
            )
        ";

        // 直接创建表
        self.connection.execute_batch(create_sql)?;

        Ok(())
    }

    /// 创建索引（延迟创建以提高插入性能）
    fn create_indexes(&self) -> DuckResult<()> {
        let index_sqls = [
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_dmlg01 ON sqllogs(session)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_dmlg02 ON sqllogs(thread)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_dmlg03 ON sqllogs(trx_id)",
        ];

        for index_sql in &index_sqls {
            self.connection.execute_batch(index_sql)?;
        }

        Ok(())
    }

    /// 将 Sqllog 记录插入到数据库
    fn insert_sqllog_batch(&self, records: &[Sqllog]) -> Result<()> {
        log::debug!("insert_sqllog_batch: 开始处理 {} 条记录", records.len());

        if records.is_empty() {
            log::debug!("insert_sqllog_batch: 记录为空，直接返回");
            return Ok(());
        }

        log::debug!("insert_sqllog_batch: 创建 Appender");
        let mut appender = self
            .connection
            .appender("sqllogs")
            .context("创建 Appender 失败")?;

        log::debug!("insert_sqllog_batch: 开始追加记录");

        // 构建所有记录的数据，避免临时值问题
        log::debug!("insert_sqllog_batch: 准备所有记录数据");
        let mut all_data: Vec<SqllogRowData> = Vec::new();

        // 复制所有数据到临时存储中，避免引用问题
        for record in records {
            all_data.push((
                record.occurrence_time.clone(), // occurrence_time CHAR(32)
                record.ep.to_string(),          // ep CHAR(1)
                record.session.clone(),         // session VARCHAR(64)
                record.thread.clone(),          // thread VARCHAR(64)
                record.user.clone(),            // username VARCHAR(128)
                record.trx_id.clone(),          // trx_id VARCHAR(64)
                record.statement.clone(),       // statement VARCHAR(64)
                record.appname.clone(),         // appname VARCHAR(256)
                record.ip.clone(),              // ip VARCHAR(45)
                record.sql_type.clone(),        // sql_type VARCHAR(32)
                record.description.clone(),     // description TEXT
                record.execute_time,            // execute_time BIGINT
                record.rowcount,                // rowcount BIGINT
                record.execute_id,              // execute_id BIGINT
            ));
        }

        // 构造引用数组用于 append_rows 一次性批量插入
        // 表列顺序：occurrence_time, ep, session, thread, username, trx_id, statement, appname, ip, sql_type, description, execute_time, rowcount, execute_id
        log::debug!("insert_sqllog_batch: 构造批量插入数据");
        let batch_rows: Vec<[&dyn duckdb::ToSql; 14]> = all_data
            .iter()
            .map(
                |(
                    occurrence_time,
                    ep,
                    session,
                    thread,
                    user,
                    trx_id,
                    statement,
                    appname,
                    ip,
                    sql_type,
                    description,
                    execute_time,
                    rowcount,
                    execute_id,
                )| {
                    [
                        occurrence_time as &dyn duckdb::ToSql, // occurrence_time CHAR(32)
                        ep as &dyn duckdb::ToSql,              // ep CHAR(1)
                        session as &dyn duckdb::ToSql, // session VARCHAR(64)
                        thread as &dyn duckdb::ToSql,  // thread VARCHAR(64)
                        user as &dyn duckdb::ToSql,    // username VARCHAR(128)
                        trx_id as &dyn duckdb::ToSql,  // trx_id VARCHAR(64)
                        statement as &dyn duckdb::ToSql, // statement VARCHAR(64)
                        appname as &dyn duckdb::ToSql,   // appname VARCHAR(256)
                        ip as &dyn duckdb::ToSql,        // ip VARCHAR(45)
                        sql_type as &dyn duckdb::ToSql,  // sql_type VARCHAR(32)
                        description as &dyn duckdb::ToSql, // description TEXT
                        execute_time as &dyn duckdb::ToSql, // execute_time BIGINT
                        rowcount as &dyn duckdb::ToSql,     // rowcount BIGINT
                        execute_id as &dyn duckdb::ToSql,   // execute_id BIGINT
                    ]
                },
            )
            .collect();

        // 使用 append_rows 一次性批量插入所有记录
        log::debug!(
            "insert_sqllog_batch: 执行批量插入 {} 条记录",
            batch_rows.len()
        );
        let result = appender.append_rows(&batch_rows);

        match result {
            Ok(()) => {
                log::debug!("insert_sqllog_batch: 批量插入成功");
                // 提交插入
                appender.flush().context("提交批量插入失败")?;
                log::debug!(
                    "insert_sqllog_batch: 成功插入 {} 条记录",
                    records.len()
                );
            }
            Err(e) => {
                log::error!("append_rows 失败: {e}");
                // 确保在错误情况下也释放 appender 资源
                drop(appender);
                return Err(e).context("批量插入失败");
            }
        }

        // 显式释放 appender 资源
        log::debug!("insert_sqllog_batch: 释放 Appender 资源");
        drop(appender);

        Ok(())
    }

    /// 批量插入记录
    /// 批量插入数据到数据库
    ///
    /// # Errors
    /// 当数据库操作失败、`append_rows` 失败或资源释放失败时返回错误
    pub fn insert_batch(&mut self, records: &[Sqllog]) -> Result<usize> {
        log::debug!("insert_batch: 开始处理 {} 条记录", records.len());

        if records.is_empty() {
            log::debug!("insert_batch: 记录为空，直接返回");
            return Ok(0);
        }

        let start = Instant::now();

        log::debug!("insert_batch: 调用 insert_sqllog_batch");
        // 直接插入所有记录，不进行分块
        self.insert_sqllog_batch(records)
            .with_context(|| format!("插入 {} 条记录失败", records.len()))?;

        let inserted = records.len();
        let duration = start.elapsed();

        #[allow(clippy::cast_precision_loss)]
        let rate = inserted as f64 / duration.as_secs_f64();

        log::debug!(
            "insert_batch: 插入完成，耗时: {duration:?}, 速度: {rate:.2} 记录/秒"
        );

        let duration_ms =
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.stats.add_insert(inserted, duration_ms);

        log::debug!("insert_batch: 成功插入 {inserted} 条记录");
        Ok(inserted)
    }

    /// 导出数据到 JSON 格式（使用 `DuckDB` COPY 命令）
    fn export_to_json(&self, output_path: &str) -> Result<()> {
        let copy_sql = format!(
            "COPY (SELECT * FROM sqllogs) TO '{}' (FORMAT JSON)",
            output_path.replace('\\', "\\\\")
        );

        self.connection
            .execute_batch(&copy_sql)
            .with_context(|| format!("无法导出 JSON 文件: {output_path}"))?;

        Ok(())
    }

    /// 导出数据到 CSV 格式（使用 `DuckDB` COPY 命令）
    fn export_to_csv(&self, output_path: &str) -> Result<()> {
        let copy_sql = format!(
            "COPY (SELECT * FROM sqllogs) TO '{}' (FORMAT CSV, HEADER)",
            output_path.replace('\\', "\\\\")
        );

        self.connection
            .execute_batch(&copy_sql)
            .with_context(|| format!("无法导出 CSV 文件: {output_path}"))?;

        Ok(())
    }

    /// 获取数据库版本
    fn get_version(&self) -> Option<String> {
        self.connection
            .query_row("SELECT version()", [], |row| row.get::<_, String>(0))
            .ok()
    }

    /// 执行原始 SQL 语句（用于数据库合并等高级操作）
    /// 执行 SQL 语句
    ///
    /// # Errors
    /// 当 SQL 执行失败时返回错误
    pub fn execute_sql(&mut self, sql: &str) -> Result<()> {
        log::debug!("执行 SQL: {sql}");
        self.connection
            .execute_batch(sql)
            .with_context(|| format!("执行 SQL 失败: {sql}"))?;
        Ok(())
    }

    /// 启用独立数据库处理模式
    pub fn enable_independent_processing(&mut self) {
        if self.independent_stats.is_none() {
            self.independent_stats = Some(Arc::new(RwLock::new(
                IndependentDatabaseStats::default(),
            )));
            self.thread_counter = Some(Arc::new(AtomicUsize::new(0)));
        }
    }

    /// 创建临时数据库文件路径
    fn create_temp_database_path(
        &self,
        base_config: &RuntimeConfig,
    ) -> Result<PathBuf> {
        let thread_counter = self
            .thread_counter
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("独立处理模式未启用"))?;

        let thread_id = thread_counter.fetch_add(1, Ordering::SeqCst);

        let base_path = if base_config.db_path.is_empty() {
            Path::new(".")
        } else {
            Path::new(&base_config.db_path)
                .parent()
                .unwrap_or_else(|| Path::new("."))
        };

        let temp_name = format!("sqllog_temp_{thread_id}.duckdb");
        Ok(base_path.join(temp_name))
    }

    /// 创建用于独立处理的临时数据库提供者
    /// 创建临时数据库提供者
    ///
    /// # Errors
    /// 当临时数据库创建失败时返回错误
    pub fn create_temp_provider(
        &self,
        base_config: &RuntimeConfig,
    ) -> Result<(Self, PathBuf)> {
        let temp_db_path = self.create_temp_database_path(base_config)?;

        let mut temp_config = base_config.clone();
        temp_config.use_in_memory = false; // 强制使用文件
        temp_config.db_path = temp_db_path.to_string_lossy().to_string();

        let temp_provider = Self::new(&temp_config)?;
        Ok((temp_provider, temp_db_path))
    }

    /// 处理单个文件到临时数据库
    /// 独立处理单个文件
    ///
    /// # Errors
    /// 当文件处理、数据库操作或文件解析失败时返回错误
    ///
    /// # Panics
    /// 当无法获取统计数据锁时会 panic
    #[allow(clippy::too_many_lines)]
    pub fn process_file_independently<P>(
        &self,
        file_path: P,
        base_config: &RuntimeConfig,
    ) -> Result<(IndependentDatabaseStats, PathBuf)>
    where
        P: AsRef<Path>,
    {
        let path = file_path.as_ref();
        log::info!(
            "process_file_independently: 开始处理文件 {}",
            path.display()
        );

        log::debug!("process_file_independently: 创建临时数据库提供者");
        let (mut temp_provider, temp_db_path) =
            self.create_temp_provider(base_config)?;

        log::info!(
            "独立处理文件 {} -> 临时数据库 {}",
            path.display(),
            temp_db_path.display()
        );

        log::debug!("process_file_independently: 初始化临时数据库");
        // 创建数据库管理器来处理文件
        temp_provider.initialize()?;

        let mut local_stats = IndependentDatabaseStats {
            temp_databases_created: 1,
            files_processed: 1,
            ..Default::default()
        };

        // 创建错误写入器（如果启用）
        let error_writer = if base_config.sqllog_write_errors {
            base_config.sqllog_errors_out_path.as_ref().map_or_else(
                || {
                    log::warn!("启用了错误写入但未指定输出路径");
                    None
                },
                |error_path| match ErrorWriter::new(error_path) {
                    Ok(writer) => {
                        log::debug!(
                            "错误写入器已启用，输出文件: {}",
                            error_path.display()
                        );
                        Some(writer)
                    }
                    Err(e) => {
                        log::error!("创建错误写入器失败: {e}，将仅记录到日志");
                        None
                    }
                },
            )
        } else {
            None
        };

        // 解析文件并插入到临时数据库
        let mut error_count = 0usize;
        let chunk_size = base_config.sqllog_chunk_size.unwrap_or(0);

        log::info!(
            "process_file_independently: 开始解析文件，chunk_size = {chunk_size}"
        );
        let parse_result = crate::sqllog::Sqllog::parse_all(
            path,
            chunk_size,
            |records| {
                log::debug!(
                    "process_file_independently: 处理 {} 条记录",
                    records.len()
                );
                match temp_provider.insert_batch(records) {
                    Ok(inserted) => {
                        local_stats.records_processed += records.len();
                        local_stats.records_inserted += inserted;
                        log::debug!(
                            "process_file_independently: 成功插入 {} 条记录，累计处理: {}",
                            inserted,
                            local_stats.records_processed
                        );
                    }
                    Err(e) => {
                        log::error!("插入记录失败: {e}");
                    }
                }
            },
            |errors| {
                error_count += errors.len();
                log::warn!(
                    "process_file_independently: 解析错误 {} 个",
                    errors.len()
                );

                // 写入错误到文件（如果启用）
                if let Some(ref writer) = error_writer {
                    writer.write_errors(path, errors);
                }
            },
        );

        if let Err(e) = parse_result {
            log::error!("process_file_independently: 解析文件失败: {e}");
            return Err(e.into());
        }

        // 完成临时数据库架构
        temp_provider.finalize_schema()?;

        if error_count > 0 {
            log::warn!(
                "文件 {} 解析完成，但有 {} 个错误",
                path.display(),
                error_count
            );
        }

        log::info!(
            "文件 {} 处理完成，插入 {} 条记录到 {}",
            path.display(),
            local_stats.records_inserted,
            temp_db_path.display()
        );

        // 更新全局统计（如果启用了独立处理）
        if let Some(stats) = &self.independent_stats {
            let mut global_stats = stats.write().unwrap();
            global_stats.records_processed += local_stats.records_processed;
            global_stats.records_inserted += local_stats.records_inserted;
            global_stats.files_processed += local_stats.files_processed;
            global_stats.temp_databases_created +=
                local_stats.temp_databases_created;
        }

        Ok((local_stats, temp_db_path))
    }

    /// 合并临时数据库到当前数据库
    ///
    /// 使用 `DuckDB` 的 ATTACH/INSERT/DETACH 模式：
    /// 1. ATTACH 'temp.duckdb' AS `temp_db`  - 附加临时数据库
    /// 2. INSERT INTO main.sqllogs SELECT * FROM `temp_db.sqllogs` - 合并数据
    /// 3. DETACH `temp_db` - 分离临时数据库
    ///
    /// 这种模式安全高效，支持大数据量合并
    /// 合并临时数据库到主数据库
    ///
    /// # Errors
    /// 当数据库附加、数据插入或分离失败时返回错误
    pub fn merge_temp_database(&mut self, temp_db_path: &Path) -> Result<()> {
        if !temp_db_path.exists() {
            log::warn!("临时数据库文件不存在: {}", temp_db_path.display());
            return Ok(());
        }

        log::info!("合并临时数据库: {}", temp_db_path.display());

        // 使用 DuckDB 的 ATTACH 和 INSERT FROM SELECT 来合并数据库
        let temp_path_str = temp_db_path.to_string_lossy();
        let attach_sql = format!("ATTACH '{temp_path_str}' AS temp_db");
        let insert_sql = "INSERT INTO sqllogs SELECT * FROM temp_db.sqllogs";
        let detach_sql = "DETACH temp_db";

        // 执行合并操作
        self.execute_sql(&attach_sql).context("ATTACH 临时数据库失败")?;

        log::debug!("正在插入数据从 {} 到主数据库", temp_db_path.display());
        self.execute_sql(insert_sql).context("插入数据到主数据库失败")?;

        self.execute_sql(detach_sql).context("DETACH 临时数据库失败")?;

        log::info!("数据库合并完成: {}", temp_db_path.display());
        Ok(())
    }

    /// 清理临时数据库文件
    /// 清理临时数据库文件
    ///
    /// # Errors
    /// 当文件删除失败时返回错误
    pub fn cleanup_temp_database(&self, temp_db_path: &Path) -> Result<()> {
        if temp_db_path.exists() {
            if let Err(e) = std::fs::remove_file(temp_db_path) {
                log::warn!(
                    "清理临时数据库文件 {} 失败: {}",
                    temp_db_path.display(),
                    e
                );
            } else {
                log::debug!("已清理临时数据库文件: {}", temp_db_path.display());
            }
        }
        Ok(())
    }

    /// 获取独立处理的统计信息
    /// 获取独立统计信息
    ///
    /// # Panics
    /// 当无法获取统计数据读锁时会 panic
    pub fn get_independent_stats(&self) -> Option<IndependentDatabaseStats> {
        self.independent_stats
            .as_ref()
            .map(|stats| stats.read().unwrap().clone())
    }
}

impl DatabaseProvider for DuckDbProvider {
    fn initialize(&mut self) -> Result<()> {
        // 只创建表，不创建索引以提高插入性能
        self.create_table().context("创建数据库表失败")?;

        self.initialized = true;
        Ok(())
    }

    fn count_records(&self) -> Result<u64> {
        let count: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM sqllogs", [], |row| row.get(0))
            .context("查询记录数失败")?;

        count.try_into().context("记录数转换失败：不能为负数")
    }

    fn export_data(
        &self,
        format: ExportFormat,
        output_path: &str,
    ) -> Result<()> {
        match format {
            ExportFormat::Json => self.export_to_json(output_path),
            ExportFormat::Csv => self.export_to_csv(output_path),
        }
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn database_info(&self) -> DatabaseInfo {
        let version = self.get_version();
        let record_count = self.count_records().unwrap_or(0);

        let mut info =
            DatabaseInfo::new(DatabaseType::DuckDb, self.mode.clone());
        if self.initialized {
            info.mark_initialized(version);
        }
        info.update_count(record_count);

        info
    }

    fn close(&mut self) -> Result<()> {
        // DuckDB 连接会在 Drop 时自动关闭
        self.initialized = false;
        Ok(())
    }

    fn finalize_schema(&mut self) -> Result<()> {
        // 在所有数据插入完成后创建索引
        self.create_indexes().context("创建索引失败")?;
        Ok(())
    }
}

impl Drop for DuckDbProvider {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// 独立数据库处理统计信息
#[derive(Debug, Default, Clone)]
pub struct IndependentDatabaseStats {
    pub records_processed: usize,
    pub records_inserted: usize,
    pub files_processed: usize,
    pub temp_databases_created: usize,
}

/// 使用独立数据库处理单个文件
/// 使用独立数据库处理单个文件
///
/// # Errors
/// 当数据库初始化、文件解析或数据处理失败时返回错误
pub fn process_file_with_independent_database<P>(
    file_path: P,
    runtime_config: &RuntimeConfig,
) -> Result<IndependentDatabaseStats>
where
    P: AsRef<Path>,
{
    // 单文件处理直接使用主数据库，不需要临时数据库和合并操作
    log::info!("单文件处理，直接使用主数据库，无需合并");

    let mut main_provider = DuckDbProvider::new(runtime_config)?;
    main_provider.initialize()?;

    let mut stats = IndependentDatabaseStats {
        files_processed: 1,
        temp_databases_created: 0, // 没有创建临时数据库
        ..Default::default()
    };

    // 创建错误写入器（如果启用）
    let error_writer = if runtime_config.sqllog_write_errors {
        runtime_config.sqllog_errors_out_path.as_ref().map_or_else(
            || {
                log::warn!("启用了错误写入但未指定输出路径");
                None
            },
            |error_path| match ErrorWriter::new(error_path) {
                Ok(writer) => {
                    log::info!(
                        "错误写入器已启用，输出文件: {}",
                        error_path.display()
                    );
                    Some(writer)
                }
                Err(e) => {
                    log::error!("创建错误写入器失败: {e}，将仅记录到日志");
                    None
                }
            },
        )
    } else {
        None
    };

    // 直接解析文件并插入到主数据库
    let mut error_count = 0usize;
    let chunk_size = runtime_config.sqllog_chunk_size.unwrap_or(0);
    let path = file_path.as_ref();

    log::info!("开始解析文件 {}，chunk_size = {}", path.display(), chunk_size);
    let parse_result = crate::sqllog::Sqllog::parse_all(
        path,
        chunk_size,
        |records| {
            log::debug!("直接处理 {} 条记录到主数据库", records.len());
            match main_provider.insert_batch(records) {
                Ok(inserted) => {
                    stats.records_processed += records.len();
                    stats.records_inserted += inserted;
                    log::debug!(
                        "成功插入 {} 条记录，累计: {}",
                        inserted,
                        stats.records_processed
                    );
                }
                Err(e) => {
                    log::error!("插入记录失败: {e}");
                }
            }
        },
        |errors| {
            error_count += errors.len();
            log::warn!("解析错误 {} 个", errors.len());

            // 写入错误到文件（如果启用）
            if let Some(ref writer) = error_writer {
                writer.write_errors(path, errors);
            }
        },
    );

    if let Err(e) = parse_result {
        log::error!("解析文件失败: {e}");
        return Err(e.into());
    }

    main_provider.finalize_schema()?;

    if error_count > 0 {
        log::warn!(
            "文件 {} 处理完成，但有 {} 个错误",
            path.display(),
            error_count
        );
    }

    log::info!("单文件处理完成: {stats:?}");
    Ok(stats)
}

/// 使用独立数据库处理多个文件
/// 使用独立数据库处理多个文件
///
/// # Errors
/// 当数据库初始化、文件解析或数据处理失败时返回错误
#[allow(clippy::too_many_lines)]
pub fn process_files_with_independent_databases<P>(
    file_paths: &[P],
    runtime_config: &RuntimeConfig,
) -> Result<IndependentDatabaseStats>
where
    P: AsRef<Path>,
{
    if file_paths.is_empty() {
        log::warn!("没有文件需要处理");
        return Ok(IndependentDatabaseStats::default());
    }

    // 如果只有一个文件，直接使用主数据库处理，不需要临时数据库和合并操作
    if file_paths.len() == 1 {
        log::info!("单文件处理，直接使用主数据库，无需合并");

        let mut main_provider = DuckDbProvider::new(runtime_config)?;
        main_provider.initialize()?;

        let file_path = &file_paths[0];
        let mut stats = IndependentDatabaseStats {
            files_processed: 1,
            temp_databases_created: 0, // 没有创建临时数据库
            ..Default::default()
        };

        // 创建错误写入器（如果启用）
        let error_writer = if runtime_config.sqllog_write_errors {
            runtime_config.sqllog_errors_out_path.as_ref().map_or_else(
                || {
                    log::warn!("启用了错误写入但未指定输出路径");
                    None
                },
                |error_path| match ErrorWriter::new(error_path) {
                    Ok(writer) => {
                        log::info!(
                            "错误写入器已启用，输出文件: {}",
                            error_path.display()
                        );
                        Some(writer)
                    }
                    Err(e) => {
                        log::error!("创建错误写入器失败: {e}，将仅记录到日志");
                        None
                    }
                },
            )
        } else {
            None
        };

        // 直接解析文件并插入到主数据库
        let mut error_count = 0usize;
        let chunk_size = runtime_config.sqllog_chunk_size.unwrap_or(0);

        log::info!(
            "开始解析文件 {}，chunk_size = {}",
            file_path.as_ref().display(),
            chunk_size
        );
        let parse_result = crate::sqllog::Sqllog::parse_all(
            file_path,
            chunk_size,
            |records| {
                log::debug!("直接处理 {} 条记录到主数据库", records.len());
                match main_provider.insert_batch(records) {
                    Ok(inserted) => {
                        stats.records_processed += records.len();
                        stats.records_inserted += inserted;
                        log::debug!(
                            "成功插入 {} 条记录，累计: {}",
                            inserted,
                            stats.records_processed
                        );
                    }
                    Err(e) => {
                        log::error!("插入记录失败: {e}");
                    }
                }
            },
            |errors| {
                error_count += errors.len();
                log::warn!("解析错误 {} 个", errors.len());

                // 写入错误到文件（如果启用）
                if let Some(ref writer) = error_writer {
                    writer.write_errors(file_path, errors);
                }
            },
        );

        if let Err(e) = parse_result {
            log::error!("解析文件失败: {e}");
            return Err(e.into());
        }

        main_provider.finalize_schema()?;

        if error_count > 0 {
            log::warn!("文件处理完成，但有 {error_count} 个错误");
        }

        log::info!("单文件处理完成: {stats:?}");
        return Ok(stats);
    }

    // 多文件处理：使用独立临时数据库
    log::info!("使用独立数据库处理 {} 个文件", file_paths.len());

    let mut main_provider = DuckDbProvider::new(runtime_config)?;
    main_provider.enable_independent_processing();
    main_provider.initialize()?;

    let mut all_temp_paths = Vec::new();
    let mut combined_stats = IndependentDatabaseStats::default();

    // 处理每个文件到独立的临时数据库
    for file_path in file_paths {
        let (file_stats, temp_path) = main_provider
            .process_file_independently(file_path, runtime_config)?;

        combined_stats.records_processed += file_stats.records_processed;
        combined_stats.records_inserted += file_stats.records_inserted;
        combined_stats.files_processed += file_stats.files_processed;
        combined_stats.temp_databases_created +=
            file_stats.temp_databases_created;

        all_temp_paths.push(temp_path);
    }

    // 合并所有临时数据库
    for temp_path in &all_temp_paths {
        main_provider.merge_temp_database(temp_path)?;
        main_provider.cleanup_temp_database(temp_path)?;
    }

    main_provider.finalize_schema()?;

    log::info!("所有数据库合并完成: {combined_stats:?}");
    Ok(combined_stats)
}

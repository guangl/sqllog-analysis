#![allow(clippy::type_complexity)]
#![allow(clippy::items_after_statements)]
use crate::sqllog::Sqllog;
use anyhow::Result;
use log::{info, trace};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// 扫描指定目录，解析所有 dmsql*.log 文件。
///
/// # 参数
/// * `dir` - 日志文件夹路径（可为 Path/PathBuf）
///
/// # 返回
/// * `Ok((文件数, 日志数, 错误文件列表))`
///   - 文件数：成功识别的 dmsql*.log 文件数量
///   - 日志数：所有文件成功解析的日志条数
///   - 错误文件列表：Vec<(文件名, 错误详情)>
///
/// # Errors
/// 目录不存在、文件读取失败、IO 错误等会返回 `Err(anyhow::Error)`。
///
/// # 行为说明
/// - 仅处理文件名以 dmsql 开头且以 .log 结尾的文件
/// - 每个文件调用 `Sqllog::from_file_with_errors` 进行分段解析
/// - 所有解析错误（格式/UTF8/IO等）均收集到 `error_files`
/// - 解析进度和耗时通过 println 输出
pub fn process_sqllog_dir<P: AsRef<Path>>(
    dir: P,
) -> Result<(usize, usize, Vec<(String, String)>, std::time::Duration)> {
    let mut total_files = 0;
    let mut total_logs = 0;
    let mut error_files = Vec::new();
    use std::time::Instant;
    let global_start = Instant::now();
    // 遍历目录下所有文件
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        // 跳过非文件（如文件夹）
        if !path.is_file() {
            continue;
        }
        // 仅处理 dmsql*.log 文件
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("dmsql")
                && std::path::Path::new(name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
            {
                total_files += 1;
                trace!("开始解析文件: {name}");
                let start = Instant::now();
                // 分段解析日志文件，收集所有错误
                let (logs, errors) = Sqllog::from_file_with_errors(&path);
                let elapsed = start.elapsed();
                trace!("文件 {name} 解析耗时: {elapsed:.2?}");
                total_logs += logs.len();
                // 错误格式化为 (文件名, 错误详情)
                for (line, content, err) in errors {
                    error_files.push((
                        name.to_string(),
                        format!("行{line}: {err}\n内容: {content}"),
                    ));
                }
            }
        }
    }
    let global_elapsed = global_start.elapsed();
    Ok((total_files, total_logs, error_files, global_elapsed))
}

/// 将所有解析失败的文件及错误详情写入 `error_files.txt`。
///
/// # 参数
/// * `error_files` - 错误文件及详情列表 Vec<(文件名, 错误详情)>
///
/// # 行为说明
/// - 若 `error_files` 为空则直接返回 Ok
/// - 否则写入 `error_files.txt`，并在控制台输出所有错误
/// - 写入失败时返回 IO 错误
///
/// # Errors
/// 文件写入失败时返回 `Err(anyhow::Error)`。
pub fn write_error_files(error_files: &[(String, String)]) -> Result<()> {
    // 无错误则无需写入
    if error_files.is_empty() {
        return Ok(());
    }
    info!("以下文件解析失败，已写入 error_files.txt:");
    // 覆盖写入 error_files.txt
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("error_files.txt")?;
    for (fname, content) in error_files {
        writeln!(file, "{fname}: {content}")?;
        info!("  {fname}: {content}");
    }
    Ok(())
}

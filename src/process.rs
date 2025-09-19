#![allow(clippy::type_complexity)]
use crate::sqllog::Sqllog;
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// 解析指定目录下所有 dmsql*.log 文件，返回总文件数、总日志数、错误文件列表
pub fn process_sqllog_dir<P: AsRef<Path>>(dir: P) -> Result<(usize, usize, Vec<(String, String)>)> {
    let mut total_files = 0;
    let mut total_logs = 0;
    let mut error_files = Vec::new();
    use std::time::Instant;
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("dmsql") && name.ends_with(".log") {
                total_files += 1;
                println!("\n开始解析文件: {name}");
                let start = Instant::now();
                // ...已移除未使用变量 file ...
                // ...已移除未使用变量...
                // ...原有分段解析逻辑已移除...
                let (logs, errors) = Sqllog::from_file_with_errors(&path);
                let elapsed = start.elapsed();
                println!("文件 {name} 解析耗时: {elapsed:.2?}");
                total_logs += logs.len();
                for (line, content, err) in errors {
                    error_files.push((
                        name.to_string(),
                        format!("行{line}: {err}\n内容: {content}"),
                    ));
                }
            }
        }
    }
    Ok((total_files, total_logs, error_files))
}

/// 错误文件写入 error_files.txt
pub fn write_error_files(error_files: &[(String, String)]) -> Result<()> {
    if error_files.is_empty() {
        return Ok(());
    }
    println!("\n以下文件解析失败，已写入 error_files.txt:");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("error_files.txt")?;
    for (fname, content) in error_files {
        writeln!(file, "{fname}: {content}")?;
        println!("  {fname}: {content}");
    }
    Ok(())
}

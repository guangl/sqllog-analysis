use anyhow::{Context, Result};
use sqllog_analysis::sqllog::Sqllog;
use std::fs::OpenOptions;
use std::io::{BufRead, Write};
use std::path::PathBuf;

fn main() -> Result<()> {
    let dir = PathBuf::from("sqllog");
    if !dir.exists() {
        println!(
            "目录 `sqllog` 不存在，当前路径: {:?}",
            std::env::current_dir()?
        );
        return Ok(());
    }

    let mut total_files = 0;
    let mut total_logs = 0;
    let mut error_files = Vec::new();

    use std::time::Instant;
    for entry in std::fs::read_dir(&dir).context("读取 sqllog 目录失败")? {
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
                // 优化分段缓冲逻辑
                let file = std::fs::File::open(&path)?;
                let reader = std::io::BufReader::new(file);
                let mut segment_buf = String::new();
                let mut segment_start_line = 1;
                let mut current_line = 0;
                let mut logs = Vec::new();
                let mut error_files_local = Vec::new();
                for line in reader.lines() {
                    current_line += 1;
                    let line = match line {
                        Ok(l) => l,
                        Err(e) => {
                            error_files_local.push((name.to_string(), format!("读取失败: {e}")));
                            continue;
                        }
                    };
                    let is_first = {
                        let prefix = line.get(0..23).unwrap_or("");
                        sqllog_analysis::sqllog::is_first_row(prefix)
                    };
                    if is_first {
                        if !segment_buf.is_empty() {
                            match Sqllog::from_line(&segment_buf, segment_start_line) {
                                Ok(Some(log)) => logs.push(log),
                                Ok(None) => {
                                    error_files_local.push((name.to_string(), segment_buf.clone()))
                                }
                                Err(e) => error_files_local.push((
                                    name.to_string(),
                                    format!("{e}\n内容: {segment_buf}"),
                                )),
                            }
                            segment_buf.clear();
                        }
                        segment_start_line = current_line;
                    }
                    if !segment_buf.is_empty() {
                        segment_buf.push('\n');
                    }
                    segment_buf.push_str(&line);
                }
                // 文件结尾最后一段
                if !segment_buf.is_empty() {
                    match Sqllog::from_line(&segment_buf, segment_start_line) {
                        Ok(Some(log)) => logs.push(log),
                        Ok(None) => error_files_local.push((name.to_string(), segment_buf.clone())),
                        Err(e) => error_files_local
                            .push((name.to_string(), format!("{e}\n内容: {segment_buf}"))),
                    }
                }
                let elapsed = start.elapsed();
                println!("文件 {name} 解析耗时: {elapsed:.2?}");
                total_logs += logs.len();
                error_files.extend(error_files_local);
            }
        }
    }

    println!(
        "\n解析完成，共处理 {total_files} 个文件，成功解析 {total_logs} 条日志。"
    );
    if !error_files.is_empty() {
        println!("\n以下文件解析失败，已写入 error_files.txt:");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("error_files.txt")?;
        for (fname, content) in &error_files {
            writeln!(file, "{fname}: {content}")?;
            println!("  {fname}: {content}");
        }
    }
    Ok(())
}

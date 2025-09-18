use anyhow::Result;
use sqllog_analysis::sqllog::Sqllog;
use std::path::Path;

fn main() -> Result<()> {
    let sqllog_dir = Path::new("sqllog");
    if !sqllog_dir.exists() {
        println!("未找到 sqllog 目录，请确认目录是否存在");
        return Ok(());
    }

    // 列出 sqllog 目录下的 dmsql*.log 文件
    println!("正在检查 sqllog 目录下的 dmsql*.log 文件...");
    for entry in std::fs::read_dir(sqllog_dir)? {
        let path = entry?.path();

        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("dmsql") && filename.ends_with(".log") {
                    println!("处理文件: {}", filename);
                    match Sqllog::from_file(&path) {
                        Ok(sqllog) => {
                            println!("成功解析文件: {}，共 {} 条记录", filename, sqllog.len());
                            // 这里可以添加更多对 sqllog 的处理逻辑
                        }
                        Err(e) => {
                            println!("解析文件 {} 时出错: {}", filename, e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

use anyhow::Result;
use sqllog_analysis::input_path::get_sqllog_dir;
use sqllog_analysis::process::{process_sqllog_dir, write_error_files};

fn main() -> Result<()> {
    let dir = get_sqllog_dir();
    if !dir.exists() {
        println!("目录不存在: {:?}", std::env::current_dir()?);
        return Ok(());
    }
    let (total_files, total_logs, error_files) = process_sqllog_dir(&dir)?;
    println!(
        "\n解析完成，共处理 {total_files} 个文件，成功解析 {total_logs} 条日志，失败解析 {} 条日志。",
        error_files.len()
    );
    write_error_files(&error_files)?;
    Ok(())
}

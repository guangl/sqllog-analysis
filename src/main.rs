use anyhow::{Context, Result};
use std::path::PathBuf;

use sqllog_analysis::sqllog::Sqllog;

fn main() -> Result<()> {
    // 允许通过命令行传入 --top N 来设置要展示的最慢 SQL 数量，默认 10
    let args: Vec<String> = std::env::args().collect();
    let top_n = args
        .windows(2)
        .find(|w| w[0] == "--top")
        .and_then(|w| w.get(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);

    let dir = PathBuf::from("sqllog");
    if !dir.exists() {
        println!(
            "目录 `sqllog` 不存在，当前路径: {:?}",
            std::env::current_dir()?
        );
        return Ok(());
    }

    let mut all_logs: Vec<Sqllog> = Vec::new();

    for entry in std::fs::read_dir(&dir).context("读取 sqllog 目录失败")? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("dmsql") && name.ends_with(".log") {
                println!("开始解析文件: {}", name);
                match Sqllog::from_file(&path) {
                    Ok(mut v) => {
                        println!("从 {} 解析到 {} 条记录", name, v.len());
                        all_logs.append(&mut v);
                    }
                    Err(e) => {
                        eprintln!("解析文件 {} 时出错: {}", name, e);
                    }
                }
            }
        }
    }

    let total = all_logs.len();
    let exec_count = all_logs.iter().filter(|l| l.execute_time.is_some()).count();
    let sum_exec: u128 = all_logs
        .iter()
        .filter_map(|l| l.execute_time)
        .map(|v| v as u128)
        .sum();
    let avg = if exec_count > 0 {
        (sum_exec / exec_count as u128) as u64
    } else {
        0
    };

    println!("\n汇总信息：");
    println!("总共解析日志条数: {}", total);
    println!(
        "带有 execute_time 的条目: {}，平均耗时: {} ms",
        exec_count, avg
    );

    // 取出带 execute_time 的记录，按耗时倒序
    let mut logs_with_time: Vec<&Sqllog> = all_logs
        .iter()
        .filter(|l| l.execute_time.is_some())
        .collect();
    logs_with_time.sort_by_key(|l| std::cmp::Reverse(l.execute_time.unwrap()));

    println!("\nTop {} 最慢 SQL:\n", top_n);
    for (i, l) in logs_with_time.into_iter().take(top_n).enumerate() {
        let stmt = l.statement.as_deref().unwrap_or("<NULL>");
        let desc_first = l.description.lines().next().unwrap_or("");
        println!(
            "{}. {} ms | {} | stmt: {}",
            i + 1,
            l.execute_time.unwrap(),
            l.occurrence_time,
            stmt
        );
        println!("   {}", truncate(desc_first, 200));
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut t = s[..max].to_string();
        t.push_str("...");
        t
    }
}

use log::{info, trace};
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

/// 获取 sqllog 文件夹路径，优先命令行参数，否则交互输入。
///
/// # 返回
/// * `PathBuf` - sqllog 文件夹路径
///
/// # Panics
/// 当交互输入无法读取或 unwrap 失败时会 panic。
#[must_use]
pub fn get_sqllog_dir() -> PathBuf {
    // 优先命令行参数
    let mut args = env::args().skip(1);
    if let Some(path) = args.next() {
        trace!("命令行参数获取 sqllog 路径: {path}");
        info!("sqllog 路径: {path}");
        return PathBuf::from(path);
    }
    // 交互输入（使用 std::io）
    print!("请输入 sqllog 文件夹路径（会查询目录下面的 dmsql*.log 文件来解析）: ");
    io::stdout().flush().ok();
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("读取输入失败");
    let input = input.trim();
    trace!("交互输入获取 sqllog 路径: {input}");
    info!("sqllog 路径: {input}");
    PathBuf::from(input)
}

use dialoguer::Input;
use log::{info, trace};
use std::env;
use std::path::PathBuf;

/// 获取 sqllog 文件夹路径，优先命令行参数，否则交互输入
pub fn get_sqllog_dir() -> PathBuf {
    // 优先命令行参数
    let mut args = env::args().skip(1);
    if let Some(path) = args.next() {
        trace!("命令行参数获取 sqllog 路径: {}", path);
        info!("sqllog 路径: {}", path);
        return PathBuf::from(path);
    }
    // 交互输入
    let input: String = Input::new()
        .with_prompt("请输入 sqllog 文件夹路径（会查询目录下面的 dmsql*.log 文件来解析）")
        .interact_text()
        .unwrap();
    trace!("交互输入获取 sqllog 路径: {}", input.trim());
    info!("sqllog 路径: {}", input.trim());
    PathBuf::from(input.trim())
}

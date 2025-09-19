use dialoguer::Input;
use std::env;
use std::path::PathBuf;

/// 获取 sqllog 文件夹路径，优先命令行参数，否则交互输入
pub fn get_sqllog_dir() -> PathBuf {
    // 优先命令行参数
    let mut args = env::args().skip(1);
    if let Some(path) = args.next() {
        return PathBuf::from(path);
    }
    // 交互输入
    let input: String = Input::new()
        .with_prompt("请输入 sqllog 文件夹路径")
        .interact_text()
        .unwrap();
    PathBuf::from(input.trim())
}

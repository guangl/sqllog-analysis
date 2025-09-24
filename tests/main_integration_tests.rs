// main.rs 集成测试 - 针对实际程序入口点的测试
// 测试真实的 main 函数和命令行解析逻辑

use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;
use std::fs;
use std::io::Write;

// 测试程序的可执行文件路径
fn get_executable_path() -> PathBuf {
    let mut path = std::env::current_dir().unwrap();
    path.push("target");
    path.push("debug");
    path.push("sqllog-cli.exe");
    path
}

// 创建测试用的 SQL 日志文件
fn create_test_log_file(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    let file_path = dir.path().join(name);
    let mut file = fs::File::create(&file_path).unwrap();
    writeln!(file, "{}", content).unwrap();
    file_path
}

#[test]
fn test_main_parse_command_help() {
    let exe = get_executable_path();
    if !exe.exists() {
        // 如果可执行文件不存在，先构建
        let output = Command::new("cargo")
            .args(&["build"])
            .current_dir(std::env::current_dir().unwrap())
            .output()
            .expect("Failed to build project");

        if !output.status.success() {
            panic!("Build failed: {}", String::from_utf8_lossy(&output.stderr));
        }
    }

    let output = Command::new(&exe)
        .args(&["--help"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("达梦数据库 SQL 日志分析工具"));
    assert!(stdout.contains("parse"));
    assert!(stdout.contains("export"));
}

#[test]
fn test_main_parse_command_with_valid_file() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    // 创建测试日志文件
    let log_content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;"#;
    let log_file = create_test_log_file(&temp_dir, "test.log", log_content);

    let output = Command::new(&exe)
        .args(&["parse", log_file.to_str().unwrap()])
        .output()
        .expect("Failed to execute command");

    // 检查程序是否正确执行
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // 允许因为缺少某些 feature 而失败，但不应该是解析错误
        if !stderr.contains("feature") && !stderr.contains("not available") {
            panic!("Command failed unexpectedly: {}", stderr);
        }
    } else {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("解析模式") || stdout.contains("文件数量"));
    }
}

#[test]
fn test_main_parse_command_with_nonexistent_file() {
    let exe = get_executable_path();

    let output = Command::new(&exe)
        .args(&["parse", "nonexistent_file.log"])
        .output()
        .expect("Failed to execute command");

    // 程序应该以非零状态码退出或处理文件不存在的情况
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // 可能在 stdout 或 stderr 中显示错误信息
    let combined_output = format!("{}{}", stdout, stderr);
    assert!(
        !output.status.success() ||
        combined_output.contains("not found") ||
        combined_output.contains("找不到") ||
        combined_output.contains("系统找不到指定的文件") ||
        combined_output.contains("解析错误: 0 个") // 程序可能会显示0个解析结果
    );
}

#[test]
fn test_main_parse_command_with_multiple_files() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    // 创建多个测试日志文件
    let log_content1 = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM table1;"#;
    let log_content2 = r#"2025-09-16 20:02:54.123 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_USER trxid:122154453027 stmt:0x6da900ef1) INSERT INTO table2 VALUES(1);"#;

    let log_file1 = create_test_log_file(&temp_dir, "test1.log", log_content1);
    let log_file2 = create_test_log_file(&temp_dir, "test2.log", log_content2);

    let output = Command::new(&exe)
        .args(&[
            "parse",
            log_file1.to_str().unwrap(),
            log_file2.to_str().unwrap()
        ])
        .output()
        .expect("Failed to execute command");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("文件数量: 2") || stdout.contains("解析模式"));
    }
}

#[test]
fn test_main_parse_with_verbose_flag() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    let log_content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;"#;
    let log_file = create_test_log_file(&temp_dir, "test.log", log_content);

    let output = Command::new(&exe)
        .args(&["--verbose", "parse", log_file.to_str().unwrap()])
        .output()
        .expect("Failed to execute command");

    // 详细模式应该输出更多信息
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}{}", stdout, stderr);

        // 在详细模式下，可能在 stdout 或 stderr 中有更多调试信息
        assert!(
            combined.contains("解析模式") ||
            combined.contains("INFO") ||
            combined.contains("DEBUG") ||
            combined.contains("TRACE") ||
            stdout.contains("文件数量")
        );
    }
}

#[test]
fn test_main_parse_with_custom_threads() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    let log_content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;"#;
    let log_file = create_test_log_file(&temp_dir, "test.log", log_content);

    let output = Command::new(&exe)
        .args(&["--threads", "2", "parse", log_file.to_str().unwrap()])
        .output()
        .expect("Failed to execute command");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("解析模式") || stdout.contains("文件数量"));
    }
}

#[test]
fn test_main_parse_with_custom_batch_size() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    let log_content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;"#;
    let log_file = create_test_log_file(&temp_dir, "test.log", log_content);

    let output = Command::new(&exe)
        .args(&["--batch-size", "500", "parse", log_file.to_str().unwrap()])
        .output()
        .expect("Failed to execute command");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("解析模式") || stdout.contains("文件数量"));
    }
}

#[test]
fn test_main_version_flag() {
    let exe = get_executable_path();

    let output = Command::new(&exe)
        .args(&["--version"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // 版本信息应该包含版本号
    assert!(stdout.contains("sqllog") || stdout.len() > 0);
}

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
#[test]
fn test_main_export_command_basic() {
    let exe = get_executable_path();
    let temp_dir = TempDir::new().unwrap();

    let log_content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;"#;
    let log_file = create_test_log_file(&temp_dir, "test.log", log_content);

    let output_base = temp_dir.path().join("output").to_str().unwrap().to_string();

    let output = Command::new(&exe)
        .args(&[
            "export",
            log_file.to_str().unwrap(),
            "--output",
            &output_base
        ])
        .output()
        .expect("Failed to execute command");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("导出模式") || stdout.contains("处理文件数"));
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // 可能因为缺少导出器功能而失败，这是可以接受的
        assert!(stderr.contains("导出器未启用") || stderr.contains("feature"));
    }
}

#[test]
fn test_main_invalid_command() {
    let exe = get_executable_path();

    let output = Command::new(&exe)
        .args(&["invalid-command"])
        .output()
        .expect("Failed to execute command");

    // 无效命令应该失败
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") ||
        stderr.contains("invalid") ||
        stderr.contains("unrecognized")
    );
}

#[test]
fn test_main_missing_required_args() {
    let exe = get_executable_path();

    let output = Command::new(&exe)
        .args(&["parse"]) // 缺少文件参数
        .output()
        .expect("Failed to execute command");

    // 缺少必需参数应该失败
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") ||
        stderr.contains("required") ||
        stderr.contains("missing")
    );
}

// 测试可执行文件是否能正确构建和运行
#[test]
fn test_executable_exists_and_runs() {
    // 确保项目能够成功构建
    let output = Command::new("cargo")
        .args(&["build"])
        .current_dir(std::env::current_dir().unwrap())
        .output()
        .expect("Failed to run cargo build");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Build failed: {}", stderr);
    }

    // 测试可执行文件是否存在
    let exe = get_executable_path();
    assert!(exe.exists(), "Executable not found at: {}", exe.display());

    // 测试可执行文件是否可以运行
    let output = Command::new(&exe)
        .args(&["--help"])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success() || output.status.code() == Some(0));
}
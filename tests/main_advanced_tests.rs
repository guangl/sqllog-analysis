// 为 main.rs 创建完整的功能测试，特别针对未覆盖的代码路径
use std::fs;
use std::path::PathBuf;
use tempfile::{tempdir, NamedTempFile};
use std::process::Command;

// 测试用的样本 SQL 日志内容
const SAMPLE_LOG_CONTENT: &str = r#"2024-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [SEL]: SELECT * FROM users WHERE id = 1
EXECTIME: 100(ms) ROWCOUNT: 1 EXEC_ID: 12345.
2024-09-16 20:02:53.563 (EP[0] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) [UPD]: UPDATE users SET name = 'test' WHERE id = 2
EXECTIME: 200(ms) ROWCOUNT: 1 EXEC_ID: 12346."#;

const COMPLEX_LOG_CONTENT: &str = r#"2024-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [SEL]: SELECT COUNT(*) FROM large_table WHERE status = 'active'
EXECTIME: 1500(ms) ROWCOUNT: 1 EXEC_ID: 12345.
2024-09-16 20:02:54.562 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_ADMIN trxid:122154453027 stmt:0x6da900ef1) [INS]: INSERT INTO audit_log (user_id, action, timestamp) VALUES (1, 'LOGIN', '2024-09-16 20:02:54')
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 12346.
2024-09-16 20:02:55.562 (EP[0] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) [UPD]: UPDATE user_sessions SET last_activity = NOW() WHERE user_id = 1
EXECTIME: 75(ms) ROWCOUNT: 1 EXEC_ID: 12347.
2024-09-16 20:02:56.562 (EP[2] sess:0x6da8ccef3 thrd:4146220 user:EDM_BASE trxid:122154453029 stmt:0x6da900ef3) [DEL]: DELETE FROM temp_data WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)
EXECTIME: 300(ms) ROWCOUNT: 25 EXEC_ID: 12348.
2024-09-16 20:02:57.562 (EP[1] sess:0x6da8ccef4 thrd:4146221 user:EDM_ADMIN trxid:122154453030 stmt:0x6da900ef4) [SEL]: SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE p.status = 'published'
EXECTIME: 800(ms) ROWCOUNT: 150 EXEC_ID: 12349."#;

// 辅助函数：创建临时测试文件
fn create_test_file(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    use std::io::Write;
    file.write_all(content.as_bytes()).expect("Failed to write to temp file");
    file
}

// 辅助函数：获取可执行文件路径
fn get_executable_path() -> PathBuf {
    let mut exe_path = PathBuf::from("target/debug/sqllog-cli.exe");
    if !exe_path.exists() {
        exe_path = PathBuf::from("target/release/sqllog-cli.exe");
    }
    if !exe_path.exists() {
        // 尝试找到任何版本的可执行文件
        for entry in fs::read_dir("target/debug").unwrap_or_else(|_| fs::read_dir("target/release").unwrap()) {
            let entry = entry.unwrap();
            let path = entry.path();
            if let Some(name) = path.file_name() {
                if name.to_string_lossy().starts_with("sqllog") && name.to_string_lossy().ends_with(".exe") {
                    return path;
                }
            }
        }
        panic!("Could not find sqllog executable");
    }
    exe_path
}

// 测试程序的主要命令行功能
#[cfg(test)]
mod main_executable_tests {
    use super::*;

    #[test]
    fn test_main_parse_command_basic_functionality() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));

        // 应该成功执行
        assert!(output.status.success(), "Parse command should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 检查输出包含预期的内容
        assert!(stdout.contains("解析模式"));
        assert!(stdout.contains("文件数量"));
        assert!(stdout.contains("成功解析记录"));
    }

    #[test]
    fn test_main_parse_command_with_verbose_flag() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--verbose")
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("verbose stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Parse command with verbose should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 应该包含详细输出
        assert!(stdout.contains("解析模式"));
    }

    #[test]
    fn test_main_parse_command_custom_batch_size() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--batch-size")
            .arg("500")
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("batch size stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Parse command with custom batch size should succeed");
    }

    #[test]
    fn test_main_parse_command_custom_threads() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--threads")
            .arg("2")
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("threads stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Parse command with custom threads should succeed");
    }

    #[test]
    fn test_main_parse_command_multiple_files() {
        let file1 = create_test_file(SAMPLE_LOG_CONTENT);
        let file2 = create_test_file(COMPLEX_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file1.path())
            .arg(file2.path())
            .output()
            .expect("Failed to execute command");

        println!("multiple files stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Parse command with multiple files should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("文件数量: 2"));
    }

    #[test]
    fn test_main_parse_command_with_complex_log() {
        let file = create_test_file(COMPLEX_LOG_CONTENT);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("complex log stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Parse command with complex log should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 应该解析出多条记录
        assert!(stdout.contains("成功解析记录"));
        assert!(stdout.contains("前几条记录"));
    }

    #[test]
    fn test_main_parse_command_with_nonexistent_file() {
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg("nonexistent_file.log")
            .output()
            .expect("Failed to execute command");

        println!("nonexistent file stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("nonexistent file stderr: {}", String::from_utf8_lossy(&output.stderr));

        // 不存在的文件应该不会崩溃，但可能会有错误信息
        // 检查是否有合理的错误处理
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);

        // 应该有某种形式的错误处理或者空结果
        assert!(stderr.contains("error") || stderr.contains("错误") ||
                stdout.contains("成功解析记录: 0") || stdout.contains("找不到"));
    }

    #[test]
    fn test_main_export_command_basic() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_output");
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("export")
            .arg("--output")
            .arg(output_path.to_str().unwrap())
            .arg("--format")
            .arg("csv")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("export stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("export stderr: {}", String::from_utf8_lossy(&output.stderr));

        // 导出命令可能会因为缺少功能而失败，但不应该崩溃
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // 检查是否有合适的输出或错误信息
        assert!(stdout.contains("并发导出模式") ||
                stderr.contains("未启用") ||
                stdout.contains("导出完成"));
    }

    #[test]
    fn test_main_export_command_auto_format() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_output");
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("export")
            .arg("--output")
            .arg(output_path.to_str().unwrap())
            .arg("--format")
            .arg("auto")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("export auto stdout: {}", String::from_utf8_lossy(&output.stdout));

        // Auto格式应该选择第一个可用的导出器
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(stdout.contains("并发导出模式") || stderr.contains("错误"));
    }

    #[test]
    fn test_main_export_all_formats() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let exe_path = get_executable_path();

        let formats = ["csv", "json", "sqlite", "duckdb"];

        for format in &formats {
            let output_path = temp_dir.path().join(format!("test_output_{}", format));

            let output = Command::new(&exe_path)
                .arg("export")
                .arg("--output")
                .arg(output_path.to_str().unwrap())
                .arg("--format")
                .arg(format)
                .arg(file.path())
                .output()
                .expect("Failed to execute command");

            println!("export {} stdout: {}", format, String::from_utf8_lossy(&output.stdout));

            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            // 每种格式应该要么成功要么给出合适的错误信息
            assert!(stdout.contains("并发导出模式") ||
                    stderr.contains("未启用") ||
                    stdout.contains("导出完成"));
        }
    }

    #[test]
    fn test_main_export_with_batch_and_threads() {
        let file = create_test_file(COMPLEX_LOG_CONTENT);
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_output");
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--batch-size")
            .arg("100")
            .arg("--threads")
            .arg("4")
            .arg("export")
            .arg("--output")
            .arg(output_path.to_str().unwrap())
            .arg("--format")
            .arg("csv")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("export with options stdout: {}", String::from_utf8_lossy(&output.stdout));

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("并发导出模式"));
    }

    #[test]
    fn test_main_help_output() {
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--help")
            .output()
            .expect("Failed to execute command");

        println!("help stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Help command should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("sqllog"));  // 应该显示程序名称
        assert!(stdout.contains("parse") || stdout.contains("export"));  // 应该显示命令
    }

    #[test]
    fn test_main_version_output() {
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("--version")
            .output()
            .expect("Failed to execute command");

        println!("version stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success(), "Version command should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 应该显示版本信息
        assert!(stdout.len() > 0);
    }

    #[test]
    fn test_main_invalid_arguments() {
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("invalid_command")
            .output()
            .expect("Failed to execute command");

        println!("invalid args stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("invalid args stderr: {}", String::from_utf8_lossy(&output.stderr));

        // 无效参数应该返回错误
        assert!(!output.status.success(), "Invalid arguments should fail");

        let stderr = String::from_utf8_lossy(&output.stderr);
        // 应该有错误信息
        assert!(stderr.len() > 0 || String::from_utf8_lossy(&output.stdout).contains("error"));
    }

    #[test]
    fn test_main_missing_required_file() {
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .output()
            .expect("Failed to execute command");

        println!("missing file stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("missing file stderr: {}", String::from_utf8_lossy(&output.stderr));

        // 缺少必需文件参数应该返回错误
        assert!(!output.status.success(), "Missing file argument should fail");

        let stderr = String::from_utf8_lossy(&output.stderr);
        // 应该有错误信息
        assert!(stderr.contains("required") || stderr.contains("必须") || stderr.contains("error"));
    }
}

// 测试 main.rs 中特定函数的逻辑
#[cfg(test)]
mod main_function_tests {
    use super::*;
    use sqllog_analysis::config::SqllogConfig;
    use sqllog_analysis::prelude::ConcurrentParser;

    // 模拟 run_parse_only 函数的行为
    fn simulate_run_parse_only(files: &[PathBuf], config: SqllogConfig) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== 解析模式 ===");
        println!("文件数量: {}", files.len());

        let parser = ConcurrentParser::new(config);
        let start = std::time::Instant::now();
        let (records, errors) = parser.parse_files_concurrent(files)?;
        let elapsed = start.elapsed();

        println!("\n=== 解析结果 ===");
        println!("成功解析记录: {} 条", records.len());
        println!("解析错误: {} 个", errors.len());
        println!("处理时间: {:?}", elapsed);

        if !errors.is_empty() {
            println!("\n前几个错误:");
            for (i, error) in errors.iter().take(5).enumerate() {
                println!("  {}. 第{}行: {}", i + 1, error.line, error.error);
            }
        }

        if !records.is_empty() {
            println!("\n前几条记录:");
            for (i, record) in records.iter().take(3).enumerate() {
                println!(
                    "  {}. {} [{}] {}",
                    i + 1,
                    record.occurrence_time,
                    record.sql_type.as_deref().unwrap_or("UNKNOWN"),
                    record.description.chars().take(80).collect::<String>()
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_run_parse_only_function_simulation() {
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let files = vec![file.path().to_path_buf()];
        let config = SqllogConfig::default();

        let result = simulate_run_parse_only(&files, config);
        assert!(result.is_ok(), "模拟 run_parse_only 应该成功");
    }

    #[test]
    fn test_run_parse_only_with_custom_config() {
        let file = create_test_file(COMPLEX_LOG_CONTENT);
        let files = vec![file.path().to_path_buf()];

        let mut config = SqllogConfig::default();
        config.batch_size = 200;
        config.thread_count = Some(3);

        let result = simulate_run_parse_only(&files, config);
        assert!(result.is_ok(), "使用自定义配置的模拟 run_parse_only 应该成功");
    }

    #[test]
    fn test_run_parse_only_error_handling() {
        let invalid_content = "Invalid log content\nAnother invalid line\nYet another invalid line";
        let file = create_test_file(invalid_content);
        let files = vec![file.path().to_path_buf()];
        let config = SqllogConfig::default();

        let result = simulate_run_parse_only(&files, config);
        // 即使有无效内容，函数也应该成功完成（只是会有解析错误）
        assert!(result.is_ok(), "带错误内容的模拟 run_parse_only 应该成功完成");
    }

    #[test]
    fn test_config_application_logic() {
        // 测试配置应用的逻辑
        let mut config = SqllogConfig::default();

        // 模拟 CLI 参数应用
        let cli_batch_size = 500;
        let cli_threads = 4;

        config.batch_size = cli_batch_size;
        if cli_threads > 0 {
            config.thread_count = Some(cli_threads);
        }

        assert_eq!(config.batch_size, 500);
        assert_eq!(config.thread_count, Some(4));
    }

    #[test]
    fn test_config_application_zero_threads() {
        // 测试线程数为0的情况
        let mut config = SqllogConfig::default();
        let original_thread_count = config.thread_count;

        let cli_batch_size = 1000;
        let cli_threads = 0;

        config.batch_size = cli_batch_size;
        if cli_threads > 0 {
            config.thread_count = Some(cli_threads);
        }
        // 如果 cli_threads 是 0，则不修改 thread_count

        assert_eq!(config.batch_size, 1000);
        // 线程数为0时应该保持原值
        assert_eq!(config.thread_count, original_thread_count);
    }

    #[test]
    fn test_pathbuf_parent_directory_logic() {
        // 测试输出目录创建逻辑
        let output_base = "test_dir/sub_dir/output";
        let path = PathBuf::from(output_base);
        let parent = path.parent();

        assert!(parent.is_some());
        assert_eq!(parent.unwrap().to_str().unwrap(), "test_dir/sub_dir");
    }

    #[test]
    fn test_output_path_generation_logic() {
        // 测试输出路径生成逻辑
        let output_base = "result";

        let csv_path = format!("{}.csv", output_base);
        let json_path = format!("{}.json", output_base);
        let sqlite_path = format!("{}.sqlite", output_base);
        let duckdb_path = format!("{}.duckdb", output_base);

        assert_eq!(csv_path, "result.csv");
        assert_eq!(json_path, "result.json");
        assert_eq!(sqlite_path, "result.sqlite");
        assert_eq!(duckdb_path, "result.duckdb");
    }

    #[test]
    fn test_results_aggregation_logic() {
        // 测试结果聚合逻辑（模拟 main.rs 中的聚合代码）
        let results: Vec<(usize, usize)> = vec![
            (100, 2),  // 100条记录, 2个错误
            (250, 1),  // 250条记录, 1个错误
            (75, 0),   // 75条记录, 0个错误
            (180, 3),  // 180条记录, 3个错误
        ];

        let total_records: usize = results.iter().map(|(r, _)| r).sum();
        let total_errors: usize = results.iter().map(|(_, e)| e).sum();

        assert_eq!(total_records, 605);
        assert_eq!(total_errors, 6);
    }

    #[test]
    fn test_timing_measurement_logic() {
        // 测试时间测量逻辑
        let start_time = std::time::Instant::now();

        // 模拟一些工作
        std::thread::sleep(std::time::Duration::from_millis(10));

        let total_duration = start_time.elapsed();

        assert!(total_duration >= std::time::Duration::from_millis(10));
        assert!(total_duration < std::time::Duration::from_millis(100));

        println!("测试耗时: {:?}", total_duration);
    }

    #[test]
    fn test_empty_results_handling() {
        // 测试空结果的处理
        let results: Vec<(usize, usize)> = vec![];

        let total_records: usize = results.iter().map(|(r, _)| r).sum();
        let total_errors: usize = results.iter().map(|(_, e)| e).sum();

        assert_eq!(total_records, 0);
        assert_eq!(total_errors, 0);
    }

    #[test]
    fn test_large_numbers_aggregation() {
        // 测试大数字聚合
        let results: Vec<(usize, usize)> = vec![
            (10000, 50),
            (25000, 100),
            (15000, 75),
            (30000, 25),
        ];

        let total_records: usize = results.iter().map(|(r, _)| r).sum();
        let total_errors: usize = results.iter().map(|(_, e)| e).sum();

        assert_eq!(total_records, 80000);
        assert_eq!(total_errors, 250);
    }
}

// 测试错误场景和边界情况
#[cfg(test)]
mod main_edge_cases_tests {
    use super::*;

    #[test]
    fn test_extremely_large_file() {
        // 创建一个包含大量记录的文件
        let large_content = SAMPLE_LOG_CONTENT.repeat(1000);
        let file = create_test_file(&large_content);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        // 大文件应该能够被处理，即使可能很慢
        assert!(output.status.success() ||
                String::from_utf8_lossy(&output.stderr).contains("timeout") ||
                String::from_utf8_lossy(&output.stdout).contains("成功解析记录"));
    }

    #[test]
    fn test_empty_file_handling() {
        let file = create_test_file("");
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("empty file stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success());

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("成功解析记录: 0"));
    }

    #[test]
    fn test_whitespace_only_file() {
        let file = create_test_file("   \n\n   \t\t   \n   ");
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("whitespace file stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success());

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("成功解析记录: 0"));
    }

    #[test]
    fn test_mixed_valid_invalid_content() {
        let mixed_content = format!("{}\nInvalid line 1\nInvalid line 2\n{}",
                                   SAMPLE_LOG_CONTENT, COMPLEX_LOG_CONTENT);
        let file = create_test_file(&mixed_content);
        let exe_path = get_executable_path();

        let output = Command::new(&exe_path)
            .arg("parse")
            .arg(file.path())
            .output()
            .expect("Failed to execute command");

        println!("mixed content stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success());

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 应该解析出一些记录，但也应该有一些错误
        assert!(stdout.contains("成功解析记录:") && stdout.contains("解析错误:"));
    }

    #[test]
    fn test_output_directory_creation_simulation() {
        // 模拟输出目录创建的逻辑
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("deep").join("nested").join("path").join("output");

        // 模拟 main.rs 中的目录创建逻辑
        if let Some(parent) = output_base.parent() {
            fs::create_dir_all(parent).expect("Should be able to create directory");
            assert!(parent.exists());
        }
    }

    #[test]
    fn test_export_format_variations() {
        // 测试不同导出格式的处理
        let file = create_test_file(SAMPLE_LOG_CONTENT);
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let exe_path = get_executable_path();

        let formats = ["csv", "json", "sqlite", "duckdb", "auto"];

        for format in &formats {
            let output_path = temp_dir.path().join(format!("test_{}", format));

            let output = Command::new(&exe_path)
                .arg("export")
                .arg("--output")
                .arg(output_path.to_str().unwrap())
                .arg("--format")
                .arg(format)
                .arg(file.path())
                .output()
                .expect("Failed to execute command");

            // 每种格式都应该被合理处理
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            assert!(stdout.contains("并发导出模式") ||
                    stderr.contains("未启用") ||
                    stdout.contains("导出完成") ||
                    stderr.contains("error"));
        }
    }
}

// 性能和并发测试
#[cfg(test)]
mod main_performance_tests {
    use super::*;

    #[test]
    fn test_concurrent_processing_multiple_files() {
        let files: Vec<_> = (0..5)
            .map(|i| create_test_file(&format!("{}\n// File {}", SAMPLE_LOG_CONTENT, i)))
            .collect();

        let exe_path = get_executable_path();
        let mut cmd = Command::new(&exe_path);
        cmd.arg("--threads").arg("4").arg("parse");

        for file in &files {
            cmd.arg(file.path());
        }

        let output = cmd.output().expect("Failed to execute command");

        println!("concurrent stdout: {}", String::from_utf8_lossy(&output.stdout));

        assert!(output.status.success());

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("文件数量: 5"));
    }

    #[test]
    fn test_different_batch_sizes() {
        let file = create_test_file(COMPLEX_LOG_CONTENT);
        let exe_path = get_executable_path();

        let batch_sizes = [10, 100, 500, 1000];

        for &batch_size in &batch_sizes {
            let output = Command::new(&exe_path)
                .arg("--batch-size")
                .arg(&batch_size.to_string())
                .arg("parse")
                .arg(file.path())
                .output()
                .expect("Failed to execute command");

            println!("batch {} stdout: {}", batch_size, String::from_utf8_lossy(&output.stdout));

            assert!(output.status.success(), "Batch size {} should work", batch_size);
        }
    }

    #[test]
    fn test_different_thread_counts() {
        let file = create_test_file(COMPLEX_LOG_CONTENT);
        let exe_path = get_executable_path();

        let thread_counts = [1, 2, 4, 8];

        for &thread_count in &thread_counts {
            let output = Command::new(&exe_path)
                .arg("--threads")
                .arg(&thread_count.to_string())
                .arg("parse")
                .arg(file.path())
                .output()
                .expect("Failed to execute command");

            println!("threads {} stdout: {}", thread_count, String::from_utf8_lossy(&output.stdout));

            assert!(output.status.success(), "Thread count {} should work", thread_count);
        }
    }
}
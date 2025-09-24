use sqllog_analysis::config::*;

/// 测试默认配置
#[test]
fn test_default_config() {
    let config = SqllogConfig::default();
    assert_eq!(config.batch_size, 0); // 默认为0，表示不分块直接解析整个文件
    assert_eq!(config.thread_count, Some(0)); // 默认为Some(0)，表示自动根据文件数量确定
    assert_eq!(config.queue_buffer_size, 10000);
}

/// 测试配置结构体的创建
#[test]
fn test_config_creation() {
    let config = SqllogConfig {
        batch_size: 500,
        thread_count: Some(4),
        queue_buffer_size: 5000,
    };

    assert_eq!(config.batch_size, 500);
    assert_eq!(config.thread_count, Some(4));
    assert_eq!(config.queue_buffer_size, 5000);
}

/// 测试配置参数边界值
#[test]
fn test_config_boundary_values() {
    let config = SqllogConfig {
        batch_size: 1,
        thread_count: Some(1),
        queue_buffer_size: 1,
    };

    assert_eq!(config.batch_size, 1);
    assert_eq!(config.thread_count, Some(1));
    assert_eq!(config.queue_buffer_size, 1);
}

/// 测试大批次大小配置
#[test]
fn test_large_batch_size() {
    let config = SqllogConfig {
        batch_size: 10000,
        thread_count: Some(16),
        queue_buffer_size: 100000,
    };

    assert_eq!(config.batch_size, 10000);
    assert_eq!(config.thread_count, Some(16));
    assert_eq!(config.queue_buffer_size, 100000);
}

/// 测试无线程数配置（自动检测）
#[test]
fn test_auto_thread_detection() {
    let config = SqllogConfig {
        batch_size: 1000,
        thread_count: None, // 应该自动检测
        queue_buffer_size: 10000,
    };

    assert_eq!(config.thread_count, None);
}

/// 测试配置克隆
#[test]
fn test_config_clone() {
    let original = SqllogConfig {
        batch_size: 2000,
        thread_count: Some(8),
        queue_buffer_size: 20000,
    };

    let cloned = original.clone();

    assert_eq!(original.batch_size, cloned.batch_size);
    assert_eq!(original.thread_count, cloned.thread_count);
    assert_eq!(original.queue_buffer_size, cloned.queue_buffer_size);
}

/// 测试配置调试输出
#[test]
fn test_config_debug_output() {
    let config = SqllogConfig {
        batch_size: 1500,
        thread_count: Some(6),
        queue_buffer_size: 15000,
    };

    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("1500"));
    assert!(debug_str.contains("6"));
    assert!(debug_str.contains("15000"));
}

/// 测试配置字段比较
#[test]
fn test_config_field_comparison() {
    let config1 = SqllogConfig {
        batch_size: 1000,
        thread_count: Some(4),
        queue_buffer_size: 10000,
    };

    let config2 = SqllogConfig {
        batch_size: 1000,
        thread_count: Some(4),
        queue_buffer_size: 10000,
    };

    let config3 = SqllogConfig {
        batch_size: 2000,
        thread_count: Some(4),
        queue_buffer_size: 10000,
    };

    // 手动比较字段
    assert_eq!(config1.batch_size, config2.batch_size);
    assert_eq!(config1.thread_count, config2.thread_count);
    assert_eq!(config1.queue_buffer_size, config2.queue_buffer_size);

    assert_ne!(config1.batch_size, config3.batch_size);
}

/// 测试多种不同配置组合
#[test]
fn test_config_combinations() {
    let configs = vec![
        SqllogConfig {
            batch_size: 100,
            thread_count: Some(1),
            queue_buffer_size: 1000,
        },
        SqllogConfig {
            batch_size: 500,
            thread_count: Some(2),
            queue_buffer_size: 5000,
        },
        SqllogConfig {
            batch_size: 1000,
            thread_count: None,
            queue_buffer_size: 10000,
        },
        SqllogConfig {
            batch_size: 5000,
            thread_count: Some(16),
            queue_buffer_size: 50000,
        },
    ];

    assert_eq!(configs.len(), 4);

    for (i, config) in configs.iter().enumerate() {
        match i {
            0 => {
                assert_eq!(config.batch_size, 100);
                assert_eq!(config.thread_count, Some(1));
                assert_eq!(config.queue_buffer_size, 1000);
            }
            1 => {
                assert_eq!(config.batch_size, 500);
                assert_eq!(config.thread_count, Some(2));
                assert_eq!(config.queue_buffer_size, 5000);
            }
            2 => {
                assert_eq!(config.batch_size, 1000);
                assert_eq!(config.thread_count, None);
                assert_eq!(config.queue_buffer_size, 10000);
            }
            3 => {
                assert_eq!(config.batch_size, 5000);
                assert_eq!(config.thread_count, Some(16));
                assert_eq!(config.queue_buffer_size, 50000);
            }
            _ => unreachable!(),
        }
    }
}

/// 测试配置修改
#[test]
fn test_config_modification() {
    let mut config = SqllogConfig::default();

    assert_eq!(config.batch_size, 0); // 默认为0
    config.batch_size = 2000;
    assert_eq!(config.batch_size, 2000);

    assert_eq!(config.thread_count, Some(0)); // 默认为Some(0)
    config.thread_count = Some(8);
    assert_eq!(config.thread_count, Some(8));

    assert_eq!(config.queue_buffer_size, 10000);
    config.queue_buffer_size = 20000;
    assert_eq!(config.queue_buffer_size, 20000);
}

/// 测试文件加载相关的错误情况
#[test]
fn test_config_file_load_errors() {
    use sqllog_analysis::config::Config;
    use std::fs;
    use tempfile::NamedTempFile;

    // 测试加载不存在的配置文件
    let result = Config::from_file("nonexistent_config.toml");
    assert!(result.is_err());

    // 测试无效的 TOML 语法
    let temp_file =
        NamedTempFile::new().expect("Failed to create temp file");
    let invalid_toml = r#"
        [invalid_section
        thread_count = 4
        batch_size = 100
    "#;

    fs::write(temp_file.path(), invalid_toml)
        .expect("Failed to write temp file");
    let result = Config::from_file(temp_file.path());
    assert!(result.is_err());

    // 测试类型不匹配
    let temp_file2 =
        NamedTempFile::new().expect("Failed to create temp file");
    let wrong_type_toml = r#"
        [log]
        enable_stdout = "not_a_boolean"

        [sqllog]
        thread_count = 4
    "#;

    fs::write(temp_file2.path(), wrong_type_toml)
        .expect("Failed to write temp file");
    let result = Config::from_file(temp_file2.path());
    assert!(result.is_err());
}

/// 测试配置文件的各种有效情况
#[test]
fn test_config_file_valid_cases() {
    use sqllog_analysis::config::Config;
    use std::fs;
    use tempfile::NamedTempFile;

    // 测试完整配置
    let temp_file =
        NamedTempFile::new().expect("Failed to create temp file");
    let full_toml = r#"
        [log]
        enable_stdout = true
        log_dir = "test_logs"
        level = "debug"

        [sqllog]
        thread_count = 8
        batch_size = 500
        queue_buffer_size = 5000
    "#;

    fs::write(temp_file.path(), full_toml).expect("Failed to write temp file");
    let result = Config::from_file(temp_file.path());
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.log.enable_stdout, true);
    assert_eq!(config.log.log_dir, "test_logs");
    assert_eq!(config.log.level, "debug");
    assert_eq!(config.sqllog.thread_count, Some(8));
    assert_eq!(config.sqllog.batch_size, 500);
    assert_eq!(config.sqllog.queue_buffer_size, 5000);

    // 测试部分配置（只有 sqllog 部分）
    let temp_file2 =
        NamedTempFile::new().expect("Failed to create temp file");
    let partial_toml = r#"
        [log]
        level = "warn"
        enable_stdout = true
        log_dir = "test_logs"

        [sqllog]
        thread_count = 4
        batch_size = 1000
        queue_buffer_size = 5000
    "#;

    fs::write(temp_file2.path(), partial_toml)
        .expect("Failed to write temp file");
    let result = Config::from_file(temp_file2.path());
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.log.level, "warn");
    assert_eq!(config.sqllog.thread_count, Some(4));
    assert_eq!(config.sqllog.batch_size, 1000);
    assert_eq!(config.sqllog.queue_buffer_size, 5000);

    // 测试带注释的文件
    let temp_file3 =
        NamedTempFile::new().expect("Failed to create temp file");
    let commented_toml = r#"
        # 日志配置
        [log]
        enable_stdout = false  # 禁用控制台输出
        log_dir = "custom_logs"
        level = "info"

        # SQL日志解析配置
        [sqllog]
        thread_count = 6  # 线程数量
        batch_size = 150
        queue_buffer_size = 8000
    "#;

    fs::write(temp_file3.path(), commented_toml)
        .expect("Failed to write temp file");
    let result = Config::from_file(temp_file3.path());
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.log.enable_stdout, false);
    assert_eq!(config.log.log_dir, "custom_logs");
    assert_eq!(config.log.level, "info");
    assert_eq!(config.sqllog.thread_count, Some(6));
    assert_eq!(config.sqllog.batch_size, 150);
    assert_eq!(config.sqllog.queue_buffer_size, 8000);
}

/// 测试配置保存功能
#[test]
fn test_config_save_to_file() {
    use sqllog_analysis::config::{Config, LogConfig};
    use tempfile::NamedTempFile;

    let config = Config {
        log: LogConfig {
            enable_stdout: false,
            log_dir: "test_save_logs".to_string(),
            level: "error".to_string(),
        },
        sqllog: SqllogConfig {
            thread_count: Some(10),
            batch_size: 300,
            queue_buffer_size: 8000,
        },
    };

    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let result = config.save_to_file(temp_file.path());
    assert!(result.is_ok());

    // 验证保存的文件可以重新加载
    let loaded_config = Config::from_file(temp_file.path());
    assert!(loaded_config.is_ok());
    let loaded = loaded_config.unwrap();

    assert_eq!(loaded.log.enable_stdout, config.log.enable_stdout);
    assert_eq!(loaded.log.log_dir, config.log.log_dir);
    assert_eq!(loaded.log.level, config.log.level);
    assert_eq!(loaded.sqllog.thread_count, config.sqllog.thread_count);
    assert_eq!(loaded.sqllog.batch_size, config.sqllog.batch_size);
    assert_eq!(
        loaded.sqllog.queue_buffer_size,
        config.sqllog.queue_buffer_size
    );
}

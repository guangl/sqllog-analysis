//! 配置模块的单元测试

use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[cfg(test)]
mod config_tests {
    use super::*;
    use sqllog_analysis::config::{Config, LogConfig, SqllogConfig};
    use sqllog_analysis::error::SqllogError;

    #[test]
    fn test_default_config() {
        let config = Config::default();

        // 验证日志配置默认值
        assert!(config.log.enable_stdout);
        assert_eq!(config.log.log_dir, "logs");
        assert_eq!(config.log.level, "info");

        // 验证 SQL 日志配置默认值
        assert_eq!(config.sqllog.thread_count, Some(0));
        assert_eq!(config.sqllog.batch_size, 0);
        assert_eq!(config.sqllog.queue_buffer_size, 10000);
    }

    #[test]
    fn test_log_config_default() {
        let log_config = LogConfig::default();
        assert!(log_config.enable_stdout);
        assert_eq!(log_config.log_dir, "logs");
        assert_eq!(log_config.level, "info");
    }

    #[test]
    fn test_sqllog_config_default() {
        let sqllog_config = SqllogConfig::default();
        assert_eq!(sqllog_config.thread_count, Some(0));
        assert_eq!(sqllog_config.batch_size, 0);
        assert_eq!(sqllog_config.queue_buffer_size, 10000);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();

        assert!(toml_str.contains("[log]"));
        assert!(toml_str.contains("[sqllog]"));
        assert!(toml_str.contains("enable_stdout = true"));
        assert!(toml_str.contains("log_dir = \"logs\""));
        assert!(toml_str.contains("level = \"info\""));
        assert!(toml_str.contains("batch_size = 0"));
    }

    #[test]
    fn test_config_deserialization() {
        let toml_content = r#"
[log]
enable_stdout = false
log_dir = "custom_logs"
level = "debug"

[sqllog]
thread_count = 4
batch_size = 1000
queue_buffer_size = 20000
"#;

        let config: Config = toml::from_str(toml_content).unwrap();

        assert!(!config.log.enable_stdout);
        assert_eq!(config.log.log_dir, "custom_logs");
        assert_eq!(config.log.level, "debug");
        assert_eq!(config.sqllog.thread_count, Some(4));
        assert_eq!(config.sqllog.batch_size, 1000);
        assert_eq!(config.sqllog.queue_buffer_size, 20000);
    }

    #[test]
    fn test_config_from_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        let toml_content = r#"
[log]
enable_stdout = true
log_dir = "test_logs"
level = "warn"

[sqllog]
thread_count = 8
batch_size = 2000
queue_buffer_size = 15000
"#;

        fs::write(&config_path, toml_content).unwrap();

        let config = Config::from_file(&config_path).unwrap();
        assert!(config.log.enable_stdout);
        assert_eq!(config.log.log_dir, "test_logs");
        assert_eq!(config.log.level, "warn");
        assert_eq!(config.sqllog.thread_count, Some(8));
        assert_eq!(config.sqllog.batch_size, 2000);
        assert_eq!(config.sqllog.queue_buffer_size, 15000);
    }

    #[test]
    fn test_config_save_to_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("save_test_config.toml");

        let mut config = Config::default();
        config.log.level = "trace".to_string();
        config.sqllog.batch_size = 5000;

        config.save_to_file(&config_path).unwrap();
        assert!(config_path.exists());

        let loaded_config = Config::from_file(&config_path).unwrap();
        assert_eq!(loaded_config.log.level, "trace");
        assert_eq!(loaded_config.sqllog.batch_size, 5000);
    }

    #[test]
    fn test_config_from_nonexistent_file() {
        let result = Config::from_file("nonexistent_file.toml");
        assert!(result.is_err());

        let error = result.unwrap_err();
        match error {
            SqllogError::Other(msg) => {
                assert!(msg.contains("读取配置文件失败"));
            }
            _ => panic!("Expected Other error type"),
        }
    }

    #[test]
    fn test_config_from_invalid_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("invalid_config.toml");

        fs::write(&config_path, "invalid toml content [[[").unwrap();

        let result = Config::from_file(&config_path);
        assert!(result.is_err());

        let error = result.unwrap_err();
        match error {
            SqllogError::Other(msg) => {
                assert!(msg.contains("解析配置文件失败"));
            }
            _ => panic!("Expected Other error type"),
        }
    }

    #[test]
    fn test_config_save_to_invalid_path() {
        let config = Config::default();
        let invalid_path = Path::new("\x00invalid_path");

        let result = config.save_to_file(invalid_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_config_values() {
        let config = Config {
            log: LogConfig {
                enable_stdout: false,
                log_dir: "custom/path".to_string(),
                level: "error".to_string(),
            },
            sqllog: SqllogConfig {
                thread_count: Some(16),
                batch_size: 10000,
                queue_buffer_size: 50000,
            },
        };

        assert!(!config.log.enable_stdout);
        assert_eq!(config.log.log_dir, "custom/path");
        assert_eq!(config.log.level, "error");
        assert_eq!(config.sqllog.thread_count, Some(16));
        assert_eq!(config.sqllog.batch_size, 10000);
        assert_eq!(config.sqllog.queue_buffer_size, 50000);
    }

    #[test]
    fn test_config_none_thread_count() {
        let toml_content = r#"
[log]
enable_stdout = true
log_dir = "logs"
level = "info"

[sqllog]
batch_size = 0
queue_buffer_size = 10000
"#;

        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.sqllog.thread_count, None);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let original_config = Config {
            log: LogConfig {
                enable_stdout: false,
                log_dir: "/var/log/sqllog".to_string(),
                level: "trace".to_string(),
            },
            sqllog: SqllogConfig {
                thread_count: Some(12),
                batch_size: 7500,
                queue_buffer_size: 25000,
            },
        };

        let serialized = toml::to_string(&original_config).unwrap();
        let deserialized: Config = toml::from_str(&serialized).unwrap();

        assert_eq!(
            original_config.log.enable_stdout,
            deserialized.log.enable_stdout
        );
        assert_eq!(original_config.log.log_dir, deserialized.log.log_dir);
        assert_eq!(original_config.log.level, deserialized.log.level);
        assert_eq!(
            original_config.sqllog.thread_count,
            deserialized.sqllog.thread_count
        );
        assert_eq!(
            original_config.sqllog.batch_size,
            deserialized.sqllog.batch_size
        );
        assert_eq!(
            original_config.sqllog.queue_buffer_size,
            deserialized.sqllog.queue_buffer_size
        );
    }

    #[test]
    fn test_config_debug_format() {
        let config = Config::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("Config"));
        assert!(debug_str.contains("LogConfig"));
        assert!(debug_str.contains("SqllogConfig"));
        assert!(debug_str.contains("enable_stdout: true"));
    }

    #[test]
    fn test_config_clone() {
        let config = Config::default();
        let cloned_config = config.clone();

        assert_eq!(config.log.enable_stdout, cloned_config.log.enable_stdout);
        assert_eq!(config.log.log_dir, cloned_config.log.log_dir);
        assert_eq!(config.log.level, cloned_config.log.level);
        assert_eq!(
            config.sqllog.thread_count,
            cloned_config.sqllog.thread_count
        );
        assert_eq!(config.sqllog.batch_size, cloned_config.sqllog.batch_size);
        assert_eq!(
            config.sqllog.queue_buffer_size,
            cloned_config.sqllog.queue_buffer_size
        );
    }

    #[test]
    fn test_config_edge_cases() {
        // 测试空字符串配置
        let toml_content = r#"
[log]
enable_stdout = true
log_dir = ""
level = ""

[sqllog]
thread_count = 0
batch_size = 0
queue_buffer_size = 0
"#;

        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.log.log_dir, "");
        assert_eq!(config.log.level, "");
        assert_eq!(config.sqllog.thread_count, Some(0));
        assert_eq!(config.sqllog.batch_size, 0);
        assert_eq!(config.sqllog.queue_buffer_size, 0);
    }
}

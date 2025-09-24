use sqllog_analysis::logging::{init_logging, init_default_logging, LogConfig};
use tracing::Level;

/// 测试默认日志初始化
#[test]
fn test_init_default_logging() {
    let result = init_default_logging();
    assert!(result.is_ok());
}

/// 测试使用默认配置的日志初始化
#[test]
fn test_init_logging_default_config() {
    let config = LogConfig::new();
    let result = init_logging(config);
    assert!(result.is_ok());
}

/// 测试使用自定义级别的日志配置
#[test]
fn test_init_logging_custom_level() {
    let config = LogConfig::new().level(Level::INFO);
    let result = init_logging(config);
    assert!(result.is_ok());
}

/// 测试多个不同级别的配置
#[test]
fn test_logging_different_levels() {
    let levels = vec![Level::ERROR, Level::WARN, Level::INFO, Level::DEBUG, Level::TRACE];

    for level in levels {
        let config = LogConfig::new().level(level);
        let result = init_logging(config);
        assert!(result.is_ok());
    }
}

/// 测试日志配置的创建和设置
#[test]
fn test_log_config_creation() {
    let config1 = LogConfig::new();
    assert_eq!(config1.level, Level::TRACE);  // 默认级别是 TRACE

    let config2 = LogConfig::new().level(Level::ERROR);
    assert_eq!(config2.level, Level::ERROR);

    let config3 = LogConfig::default();
    assert_eq!(config3.level, Level::TRACE);
}

/// 测试日志配置的克隆
#[test]
fn test_log_config_clone() {
    let original = LogConfig::new().level(Level::WARN);
    let cloned = original.clone();

    assert_eq!(original.level, cloned.level);
}

/// 测试日志配置的调试输出
#[test]
fn test_log_config_debug() {
    let config = LogConfig::new().level(Level::INFO);
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("LogConfig"));
    // Level::INFO 可能显示为 "INFO" 或包含级别信息
    assert!(debug_str.contains("INFO") || debug_str.to_uppercase().contains("INFO"));
}

/// 测试多次初始化日志系统
#[test]
fn test_multiple_logging_initialization() {
    // 第一次初始化
    let result1 = init_default_logging();
    assert!(result1.is_ok());

    // 再次初始化应该也能成功（应该被忽略）
    let result2 = init_default_logging();
    assert!(result2.is_ok());

    // 使用不同配置再次初始化
    let config = LogConfig::new().level(Level::ERROR);
    let result3 = init_logging(config);
    assert!(result3.is_ok());
}

/// 测试日志配置链式调用
#[test]
fn test_log_config_chaining() {
    let config = LogConfig::new()
        .level(Level::DEBUG);

    assert_eq!(config.level, Level::DEBUG);
}

/// 测试各种级别的日志配置
#[test]
fn test_all_log_levels() {
    let error_config = LogConfig::new().level(Level::ERROR);
    assert_eq!(error_config.level, Level::ERROR);

    let warn_config = LogConfig::new().level(Level::WARN);
    assert_eq!(warn_config.level, Level::WARN);

    let info_config = LogConfig::new().level(Level::INFO);
    assert_eq!(info_config.level, Level::INFO);

    let debug_config = LogConfig::new().level(Level::DEBUG);
    assert_eq!(debug_config.level, Level::DEBUG);

    let trace_config = LogConfig::new().level(Level::TRACE);
    assert_eq!(trace_config.level, Level::TRACE);
}

/// 测试日志系统的性能
#[test]
fn test_logging_performance() {
    let start = std::time::Instant::now();
    let result = init_default_logging();
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    // 初始化应该很快（少于1秒）
    assert!(elapsed.as_secs() < 1);
}

/// 测试日志目录创建
#[test]
fn test_logging_creates_directory() {
    // 初始化日志系统应该创建 logs 目录
    let result = init_default_logging();
    assert!(result.is_ok());

    // 等待一点时间让目录被创建
    std::thread::sleep(std::time::Duration::from_millis(100));

    // 检查 logs 目录是否被创建
    let logs_dir = std::path::Path::new("logs");
    assert!(logs_dir.exists());
}

/// 测试配置的方法调用
#[test]
fn test_config_methods() {
    // 测试 new() 方法
    let config1 = LogConfig::new();
    assert_eq!(config1.level, Level::TRACE);

    // 测试 default() 方法
    let config2 = LogConfig::default();
    assert_eq!(config2.level, Level::TRACE);

    // 测试 level() 方法
    let config3 = LogConfig::new().level(Level::WARN);
    assert_eq!(config3.level, Level::WARN);
}
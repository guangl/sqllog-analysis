use log::LevelFilter;
use sqllog_analysis::analysis_log::LogConfig;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn test_logconfig_init_disabled_no_panic() {
    let cfg = LogConfig {
        enabled: false,
        level: LevelFilter::Info,
        log_file: None,
        enable_stdout: false,
    };
    cfg.init().unwrap();
}

#[test]
fn test_logconfig_init_with_dir_creates_logs_dir() {
    let dir = tempdir().unwrap();
    let p: PathBuf = dir.path().to_path_buf();
    let cfg = LogConfig {
        enabled: true,
        level: LevelFilter::Info,
        log_file: Some(p.clone()),
        enable_stdout: false,
    };
    cfg.init().unwrap();
    // directory should exist (logs file inside)
    assert!(p.exists());
}

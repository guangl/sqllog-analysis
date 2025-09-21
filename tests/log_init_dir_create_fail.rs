use log::LevelFilter;
use sqllog_analysis::analysis_log::LogConfig;
use std::fs::File;
use tempfile::tempdir;

// This test creates a situation where the parent path that should be a directory
// is pre-created as a regular file, so create_dir_all on that path will fail on most OSes.
#[test]
fn test_logconfig_init_dir_create_fail() {
    let dir = tempdir().unwrap();
    let base = dir.path().to_path_buf();

    // Create a file that will conflict with the directory we want to create.
    // Suppose log_file is Some(base.join("parent_dir")), and init will try to create
    // base.join("parent_dir").join("sqllog-analysis-<date>.log")'s parent.
    // We'll create a file at base.join("parent_dir") so create_dir_all(parent) fails.
    let parent_file = base.join("parent_dir");
    File::create(&parent_file).unwrap();

    let cfg = LogConfig {
        enabled: true,
        level: LevelFilter::Info,
        log_file: Some(parent_file),
        enable_stdout: false,
    };

    let res = cfg.init();
    assert!(res.is_err(), "expected init() to fail when parent path is a file");

    // cleanup: tempdir will be removed automatically
}

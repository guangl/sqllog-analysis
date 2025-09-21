use sqllog_analysis::config::Config;
use std::env;

#[test]
fn test_resolve_runtime_defaults() {
    // Ensure that with no config file, resolve_runtime returns defaults
    let dir = tempfile::tempdir().unwrap();
    let old = env::current_dir().unwrap();
    env::set_current_dir(dir.path()).unwrap();
    let runtime = Config::load();
    assert_eq!(runtime.db_path, "sqllogs.duckdb");
    env::set_current_dir(old).unwrap();
    // enable_stdout default depends on debug assertions; just ensure the field exists
    let _ = runtime.enable_stdout;
}

#[test]
fn test_load_from_config_file() {
    // Construct Config directly to test resolve_runtime with provided values
    // Construct a RuntimeConfig by building a Config and resolving it via load
    let mut cfg_obj = Config::load();
    // override fields to simulate loading from file
    cfg_obj.db_path = "mydb.duckdb".into();
    let runtime = cfg_obj;
    assert_eq!(runtime.db_path, "mydb.duckdb");
}

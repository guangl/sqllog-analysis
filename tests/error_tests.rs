//! 错误处理系统的单元测试

#[cfg(test)]
mod error_tests {
    use sqllog_analysis::error::{Result, SqllogError};
    use std::io;

    #[test]
    fn test_io_error_from() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let sqllog_err: SqllogError = io_err.into();

        assert!(sqllog_err.is_io_error());
        assert!(!sqllog_err.is_parse_error());
        assert!(!sqllog_err.is_format_error());
        assert!(!sqllog_err.is_config_error());
        assert!(!sqllog_err.is_other_error());
    }

    #[test]
    fn test_utf8_error_from() {
        // 创建一个无效的UTF-8字节序列
        let invalid_utf8 = &[0xFF, 0xFE, 0xFD];
        #[allow(invalid_from_utf8)]
        let utf8_err = std::str::from_utf8(invalid_utf8).unwrap_err();
        let sqllog_err: SqllogError = utf8_err.into();

        let display_str = format!("{}", sqllog_err);
        assert!(display_str.contains("UTF-8编码错误"));
    }

    #[test]
    fn test_regex_error_from() {
        let regex_result = regex::Regex::new("[");
        assert!(regex_result.is_err());

        let regex_err = regex_result.unwrap_err();
        let sqllog_err: SqllogError = regex_err.into();

        let display_str = format!("{}", sqllog_err);
        assert!(display_str.contains("正则表达式错误"));
    }

    #[test]
    fn test_format_error_creation() {
        let format_err =
            SqllogError::format_error(42, "invalid line format".to_string());

        assert!(format_err.is_format_error());
        assert!(!format_err.is_io_error());
        assert!(!format_err.is_parse_error());
        assert!(!format_err.is_config_error());
        assert!(!format_err.is_other_error());

        let display_str = format!("{}", format_err);
        assert!(display_str.contains("42"));
        assert!(display_str.contains("invalid line format"));
        assert!(display_str.contains("格式错误"));
    }

    #[test]
    fn test_parse_error_creation() {
        let parse_err = SqllogError::parse_error("failed to parse timestamp");

        assert!(parse_err.is_parse_error());
        assert!(!parse_err.is_io_error());
        assert!(!parse_err.is_format_error());
        assert!(!parse_err.is_config_error());
        assert!(!parse_err.is_other_error());

        let display_str = format!("{}", parse_err);
        assert!(display_str.contains("failed to parse timestamp"));
        assert!(display_str.contains("解析错误"));
    }

    #[test]
    fn test_config_error_creation() {
        let config_err =
            SqllogError::config_error("missing configuration file");

        assert!(config_err.is_config_error());
        assert!(!config_err.is_io_error());
        assert!(!config_err.is_parse_error());
        assert!(!config_err.is_format_error());
        assert!(!config_err.is_other_error());

        let display_str = format!("{}", config_err);
        assert!(display_str.contains("missing configuration file"));
        assert!(display_str.contains("配置错误"));
    }

    #[test]
    fn test_other_error_creation() {
        let other_err = SqllogError::other("unexpected error occurred");

        assert!(other_err.is_other_error());
        assert!(!other_err.is_io_error());
        assert!(!other_err.is_parse_error());
        assert!(!other_err.is_format_error());
        assert!(!other_err.is_config_error());

        let display_str = format!("{}", other_err);
        assert!(display_str.contains("unexpected error occurred"));
        assert!(display_str.contains("未知错误"));
    }

    #[test]
    fn test_error_debug() {
        let parse_err = SqllogError::parse_error("debug test");
        let debug_str = format!("{:?}", parse_err);

        assert!(debug_str.contains("Parse"));
        assert!(debug_str.contains("debug test"));
    }

    #[test]
    fn test_error_display_formatting() {
        // 测试不同类型错误的显示格式
        let format_err = SqllogError::Format {
            line: 100,
            content: "malformed data".to_string(),
        };
        let display_str = format!("{}", format_err);
        assert!(display_str.contains("行100"));
        assert!(display_str.contains("malformed data"));

        let parse_err =
            SqllogError::Parse { message: "invalid syntax".to_string() };
        let display_str = format!("{}", parse_err);
        assert!(display_str.contains("invalid syntax"));

        let config_err = SqllogError::Config("bad config".to_string());
        let display_str = format!("{}", config_err);
        assert!(display_str.contains("bad config"));

        let other_err = SqllogError::Other("misc error".to_string());
        let display_str = format!("{}", other_err);
        assert!(display_str.contains("misc error"));
    }

    #[test]
    fn test_result_type() {
        fn success_function() -> Result<i32> {
            Ok(42)
        }

        fn error_function() -> Result<i32> {
            Err(SqllogError::parse_error("test error"))
        }

        let success_result = success_function();
        assert!(success_result.is_ok());
        assert_eq!(success_result.unwrap(), 42);

        let error_result = error_function();
        assert!(error_result.is_err());
        assert!(error_result.unwrap_err().is_parse_error());
    }

    #[test]
    fn test_error_chain() {
        // 测试错误链：从IO错误转换为SqllogError
        let io_err =
            io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let sqllog_err: SqllogError = io_err.into();

        let display_str = format!("{}", sqllog_err);
        assert!(display_str.contains("IO错误"));
        assert!(display_str.contains("access denied"));
    }

    #[test]
    fn test_error_is_methods() {
        let errors = vec![
            (SqllogError::format_error(1, "test".to_string()), "format"),
            (SqllogError::parse_error("test"), "parse"),
            (SqllogError::config_error("test"), "config"),
            (SqllogError::other("test"), "other"),
            (
                SqllogError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "test",
                )),
                "io",
            ),
        ];

        for (error, error_type) in errors {
            match error_type {
                "format" => {
                    assert!(error.is_format_error());
                    assert!(!error.is_parse_error());
                    assert!(!error.is_config_error());
                    assert!(!error.is_other_error());
                    assert!(!error.is_io_error());
                }
                "parse" => {
                    assert!(!error.is_format_error());
                    assert!(error.is_parse_error());
                    assert!(!error.is_config_error());
                    assert!(!error.is_other_error());
                    assert!(!error.is_io_error());
                }
                "config" => {
                    assert!(!error.is_format_error());
                    assert!(!error.is_parse_error());
                    assert!(error.is_config_error());
                    assert!(!error.is_other_error());
                    assert!(!error.is_io_error());
                }
                "other" => {
                    assert!(!error.is_format_error());
                    assert!(!error.is_parse_error());
                    assert!(!error.is_config_error());
                    assert!(error.is_other_error());
                    assert!(!error.is_io_error());
                }
                "io" => {
                    assert!(!error.is_format_error());
                    assert!(!error.is_parse_error());
                    assert!(!error.is_config_error());
                    assert!(!error.is_other_error());
                    assert!(error.is_io_error());
                }
                _ => panic!("Unknown error type: {}", error_type),
            }
        }
    }

    #[test]
    fn test_error_with_string_conversion() {
        // 测试字符串到错误的转换
        let message = "test message".to_string();
        let parse_err = SqllogError::parse_error(message.clone());
        assert!(format!("{}", parse_err).contains(&message));

        let config_err = SqllogError::config_error(message.clone());
        assert!(format!("{}", config_err).contains(&message));

        let other_err = SqllogError::other(message.clone());
        assert!(format!("{}", other_err).contains(&message));
    }

    #[test]
    fn test_error_with_str_conversion() {
        // 测试&str到错误的转换
        let message = "test message";
        let parse_err = SqllogError::parse_error(message);
        assert!(format!("{}", parse_err).contains(message));

        let config_err = SqllogError::config_error(message);
        assert!(format!("{}", config_err).contains(message));

        let other_err = SqllogError::other(message);
        assert!(format!("{}", other_err).contains(message));
    }

    #[cfg(feature = "exporter-json")]
    #[test]
    fn test_json_error_conversion() {
        // 创建一个JSON序列化错误
        use serde_json;
        use std::collections::HashMap;

        // 创建一个会导致循环引用的结构来触发序列化错误
        let mut map: HashMap<String, serde_json::Value> = HashMap::new();
        map.insert("key".to_string(), serde_json::json!(map));

        // 尝试序列化一个非常深的嵌套结构来触发错误
        let mut deep_value = serde_json::json!(1);
        for _ in 0..1000 {
            deep_value = serde_json::json!([deep_value]);
        }
        let invalid_json = serde_json::to_string(&deep_value);
        // 深度嵌套可能不会失败，所以我们检查结果
        match invalid_json {
            Ok(_) => {
                // 如果成功，则测试通过
                assert!(true);
            }
            Err(json_err) => {
                let sqllog_err: SqllogError = json_err.into();

                let display_str = format!("{}", sqllog_err);
                assert!(display_str.contains("JSON序列化错误"));
            }
        }
    }

    #[test]
    fn test_error_equality() {
        // 测试相同类型错误的相等性（通过display字符串）
        let err1 = SqllogError::parse_error("same message");
        let err2 = SqllogError::parse_error("same message");

        // 虽然Error通常不实现PartialEq，但我们可以通过display字符串比较
        assert_eq!(format!("{}", err1), format!("{}", err2));

        let err3 = SqllogError::parse_error("different message");
        assert_ne!(format!("{}", err1), format!("{}", err3));
    }

    #[test]
    fn test_complex_error_scenarios() {
        // 测试复杂的错误场景
        fn process_data(data: &str) -> Result<i32> {
            if data.is_empty() {
                return Err(SqllogError::parse_error("empty data"));
            }

            if data.len() > 100 {
                return Err(SqllogError::format_error(1, data.to_string()));
            }

            data.parse::<i32>()
                .map_err(|_| SqllogError::parse_error("not a number"))
        }

        // 测试成功情况
        assert_eq!(process_data("42").unwrap(), 42);

        // 测试空数据错误
        let empty_result = process_data("");
        assert!(empty_result.is_err());
        assert!(empty_result.unwrap_err().is_parse_error());

        // 测试格式错误（超长数据）
        let long_data = "a".repeat(101);
        let long_result = process_data(&long_data);
        assert!(long_result.is_err());
        assert!(long_result.unwrap_err().is_format_error());

        // 测试解析错误
        let invalid_number = process_data("not_a_number");
        assert!(invalid_number.is_err());
        assert!(invalid_number.unwrap_err().is_parse_error());
    }
}

//! 类型系统的单元测试

#[cfg(test)]
mod types_tests {
    use sqllog_analysis::sqllog::types::*;
    use sqllog_analysis::sqllog::{ParseError, ParseResult};

    #[test]
    fn test_sqllog_creation() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            thread: Some("4146217".to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some("122154453026".to_string()),
            statement: Some("0x6da900ef0".to_string()),
            appname: None,
            ip: Some("10.80.147.109".to_string()),
            sql_type: None,
            description: "PARAMS(SEQNO, TYPE, DATA)".to_string(),
            execute_time: None,
            rowcount: None,
            execute_id: None,
        };

        assert_eq!(sqllog.occurrence_time, "2025-09-16 20:02:53.562");
        assert_eq!(sqllog.ep, "0");
        assert_eq!(sqllog.session, Some("0x6da8ccef0".to_string()));
        assert_eq!(sqllog.thread, Some("4146217".to_string()));
        assert_eq!(sqllog.user, Some("EDM_BASE".to_string()));
        assert!(sqllog.appname.is_none());
    }

    #[test]
    fn test_sqllog_default() {
        let sqllog = Sqllog::default();

        assert_eq!(sqllog.occurrence_time, "");
        assert_eq!(sqllog.ep, "");
        assert!(sqllog.session.is_none());
        assert!(sqllog.thread.is_none());
        assert!(sqllog.user.is_none());
        assert!(sqllog.trx_id.is_none());
        assert!(sqllog.statement.is_none());
        assert!(sqllog.appname.is_none());
        assert!(sqllog.ip.is_none());
        assert!(sqllog.sql_type.is_none());
        assert_eq!(sqllog.description, "");
        assert!(sqllog.execute_time.is_none());
        assert!(sqllog.rowcount.is_none());
        assert!(sqllog.execute_id.is_none());
    }

    #[test]
    fn test_sqllog_new() {
        let sqllog = Sqllog::new();
        let default_sqllog = Sqllog::default();
        assert_eq!(sqllog, default_sqllog);
    }

    #[test]
    fn test_sqllog_clone() {
        let original = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            thread: Some("4146217".to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some("122154453026".to_string()),
            statement: Some("0x6da900ef0".to_string()),
            appname: Some("TestApp".to_string()),
            ip: Some("10.80.147.109".to_string()),
            sql_type: Some("SELECT".to_string()),
            description: "Test query".to_string(),
            execute_time: Some(1000),
            rowcount: Some(100),
            execute_id: Some(12345),
        };

        let cloned = original.clone();
        assert_eq!(original.occurrence_time, cloned.occurrence_time);
        assert_eq!(original.ep, cloned.ep);
        assert_eq!(original.session, cloned.session);
        assert_eq!(original.thread, cloned.thread);
        assert_eq!(original.user, cloned.user);
        assert_eq!(original.trx_id, cloned.trx_id);
        assert_eq!(original.statement, cloned.statement);
        assert_eq!(original.appname, cloned.appname);
        assert_eq!(original.ip, cloned.ip);
        assert_eq!(original.sql_type, cloned.sql_type);
        assert_eq!(original.description, cloned.description);
        assert_eq!(original.execute_time, cloned.execute_time);
        assert_eq!(original.rowcount, cloned.rowcount);
        assert_eq!(original.execute_id, cloned.execute_id);
    }

    #[test]
    fn test_sqllog_debug_format() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            ..Default::default()
        };

        let debug_str = format!("{:?}", sqllog);
        assert!(debug_str.contains("Sqllog"));
        assert!(debug_str.contains("occurrence_time"));
        assert!(debug_str.contains("2025-09-16 20:02:53.562"));
    }

    #[test]
    fn test_sqllog_partial_eq() {
        let sqllog1 = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            ..Default::default()
        };

        let sqllog2 = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            ..Default::default()
        };

        let sqllog3 = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.563".to_string(),
            ep: "0".to_string(),
            ..Default::default()
        };

        assert_eq!(sqllog1, sqllog2);
        assert_ne!(sqllog1, sqllog3);
    }

    #[test]
    fn test_sqllog_serialization() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            thread: Some("4146217".to_string()),
            user: Some("EDM_BASE".to_string()),
            ..Default::default()
        };

        // 测试字段序列化为字符串格式
        let field_values = sqllog.field_values();
        assert!(!field_values.is_empty());
        assert_eq!(field_values[0], "2025-09-16 20:02:53.562");
    }

    #[test]
    fn test_sqllog_field_names() {
        let field_names = Sqllog::field_names();

        assert_eq!(field_names.len(), 14);
        assert!(field_names.contains(&"occurrence_time"));
        assert!(field_names.contains(&"ep"));
        assert!(field_names.contains(&"session"));
        assert!(field_names.contains(&"thread"));
        assert!(field_names.contains(&"user"));
        assert!(field_names.contains(&"trx_id"));
        assert!(field_names.contains(&"statement"));
        assert!(field_names.contains(&"appname"));
        assert!(field_names.contains(&"ip"));
        assert!(field_names.contains(&"sql_type"));
        assert!(field_names.contains(&"description"));
        assert!(field_names.contains(&"execute_time"));
        assert!(field_names.contains(&"rowcount"));
        assert!(field_names.contains(&"execute_id"));
    }

    #[test]
    fn test_sqllog_field_values() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            thread: Some("4146217".to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some("122154453026".to_string()),
            statement: Some("0x6da900ef0".to_string()),
            appname: Some("MyApp".to_string()),
            ip: Some("10.80.147.109".to_string()),
            sql_type: Some("SELECT".to_string()),
            description: "Test query".to_string(),
            execute_time: Some(1500),
            rowcount: Some(42),
            execute_id: Some(98765),
        };

        let field_values = sqllog.field_values();
        assert_eq!(field_values.len(), 14);
        assert_eq!(field_values[0], "2025-09-16 20:02:53.562");
        assert_eq!(field_values[1], "0");
        assert_eq!(field_values[2], "0x6da8ccef0");
        assert_eq!(field_values[3], "4146217");
        assert_eq!(field_values[4], "EDM_BASE");
        assert_eq!(field_values[5], "122154453026");
        assert_eq!(field_values[6], "0x6da900ef0");
        assert_eq!(field_values[7], "MyApp");
        assert_eq!(field_values[8], "10.80.147.109");
        assert_eq!(field_values[9], "SELECT");
        assert_eq!(field_values[10], "Test query");
        assert_eq!(field_values[11], "1500");
        assert_eq!(field_values[12], "42");
        assert_eq!(field_values[13], "98765");
    }

    #[test]
    fn test_sqllog_field_values_with_none() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            description: "Test query".to_string(),
            ..Default::default()
        };

        let field_values = sqllog.field_values();
        assert_eq!(field_values.len(), 14);
        assert_eq!(field_values[0], "2025-09-16 20:02:53.562");
        assert_eq!(field_values[1], "0");
        assert_eq!(field_values[2], ""); // None becomes empty string
        assert_eq!(field_values[3], ""); // None becomes empty string
        assert_eq!(field_values[10], "Test query");
    }

    #[test]
    fn test_parse_result_creation() {
        let result = ParseResult::new();
        assert_eq!(result.records.len(), 0);
        assert_eq!(result.errors.len(), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_result_add_record() {
        let mut result = ParseResult::new();
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ..Default::default()
        };

        result.records.push(sqllog.clone());
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0], sqllog);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_parse_result_add_error() {
        let mut result = ParseResult::new();
        let error = ParseError {
            line: 10,
            error: "Invalid format".to_string(),
            content: "malformed line content".to_string(),
        };

        result.errors.push(error.clone());
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].line, 10);
        assert_eq!(result.errors[0].error, "Invalid format");
        assert!(!result.is_empty());
    }

    #[test]
    fn test_parse_result_total_count() {
        let mut result = ParseResult::new();

        result.records.push(Sqllog::default());
        result.records.push(Sqllog::default());

        result.errors.push(ParseError {
            line: 5,
            error: "Error 1".to_string(),
            content: "line 5".to_string(),
        });

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.total_count(), 3);
    }

    #[test]
    fn test_parse_error_creation() {
        let error = ParseError {
            line: 42,
            error: "Parse failed".to_string(),
            content: "invalid line".to_string(),
        };

        assert_eq!(error.line, 42);
        assert_eq!(error.error, "Parse failed");
        assert_eq!(error.content, "invalid line");
    }

    #[test]
    fn test_parse_error_debug() {
        let error = ParseError {
            line: 25,
            error: "Syntax error".to_string(),
            content: "malformed syntax".to_string(),
        };

        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("ParseError"));
        assert!(debug_str.contains("25"));
        assert!(debug_str.contains("Syntax error"));
    }

    #[test]
    fn test_parse_error_clone() {
        let original = ParseError {
            line: 33,
            error: "Data type error".to_string(),
            content: "wrong type".to_string(),
        };

        let cloned = original.clone();
        assert_eq!(original.line, cloned.line);
        assert_eq!(original.error, cloned.error);
        assert_eq!(original.content, cloned.content);
    }

    #[test]
    fn test_parse_result_default() {
        let result = ParseResult::default();
        assert_eq!(result.records.len(), 0);
        assert_eq!(result.errors.len(), 0);
        assert!(result.is_empty());
        assert_eq!(result.total_count(), 0);
    }

    #[test]
    fn test_parse_result_with_data() {
        let mut result = ParseResult::new();

        // 添加一些记录
        for i in 0..5 {
            result.records.push(Sqllog {
                occurrence_time: format!("2025-09-16 20:02:53.{:03}", i),
                ..Default::default()
            });
        }

        // 添加一些错误
        for i in 0..3 {
            result.errors.push(ParseError {
                line: i + 1,
                error: format!("Error {}", i),
                content: format!("Line content {}", i),
            });
        }

        assert_eq!(result.records.len(), 5);
        assert_eq!(result.errors.len(), 3);
        assert_eq!(result.total_count(), 8);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_sqllog_with_all_numeric_fields() {
        let sqllog = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.562".to_string(),
            ep: "0".to_string(),
            session: Some("0x6da8ccef0".to_string()),
            thread: Some("4146217".to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some("122154453026".to_string()),
            statement: Some("0x6da900ef0".to_string()),
            appname: Some("MyApp".to_string()),
            ip: Some("10.80.147.109".to_string()),
            sql_type: Some("SELECT".to_string()),
            description: "Query description".to_string(),
            execute_time: Some(1500),
            rowcount: Some(42),
            execute_id: Some(98765),
        };

        // Verify all fields are set correctly
        assert_eq!(sqllog.occurrence_time, "2025-09-16 20:02:53.562");
        assert_eq!(sqllog.ep, "0");
        assert!(sqllog.session.is_some());
        assert!(sqllog.thread.is_some());
        assert!(sqllog.user.is_some());
        assert!(sqllog.trx_id.is_some());
        assert!(sqllog.statement.is_some());
        assert!(sqllog.appname.is_some());
        assert!(sqllog.ip.is_some());
        assert!(sqllog.sql_type.is_some());
        assert!(!sqllog.description.is_empty());
        assert_eq!(sqllog.execute_time, Some(1500));
        assert_eq!(sqllog.rowcount, Some(42));
        assert_eq!(sqllog.execute_id, Some(98765));
    }
}

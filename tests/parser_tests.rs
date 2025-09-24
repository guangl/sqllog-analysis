//! SQL日志解析器的单元测试

#[cfg(test)]
mod parser_tests {
    use sqllog_analysis::sqllog::parser::SqllogParser;

    #[test]
    fn test_parse_segment_valid() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) PARAMS(SEQNO, TYPE, DATA)";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.occurrence_time, "2025-09-16 20:02:53.562");
        assert_eq!(sqllog.ep, "0");
        assert_eq!(sqllog.session, Some("0x6da8ccef0".to_string()));
        assert_eq!(sqllog.thread, Some("4146217".to_string()));
        assert_eq!(sqllog.user, Some("EDM_BASE".to_string()));
        assert_eq!(sqllog.trx_id, Some("122154453026".to_string()));
        assert_eq!(sqllog.statement, Some("0x6da900ef0".to_string()));
        assert_eq!(sqllog.description, "PARAMS(SEQNO, TYPE, DATA)");
    }

    #[test]
    fn test_parse_segment_with_sql_type() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [SEL]: SELECT * FROM users";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.sql_type, Some("SEL".to_string()));
        assert_eq!(sqllog.description, "SELECT * FROM users");
    }

    #[test]
    fn test_parse_segment_with_appname() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname:MyApplication) TEST QUERY";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.appname, Some("MyApplication".to_string()));
        assert_eq!(sqllog.description, "TEST QUERY");
    }

    #[test]
    fn test_parse_segment_with_ip() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 ip:::ffff:192.168.1.100) TEST QUERY";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.ip, Some("192.168.1.100".to_string()));
        assert_eq!(sqllog.description, "TEST QUERY");
    }

    #[test]
    fn test_parse_segment_with_null_values() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) TEST QUERY";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.session, None);
        assert_eq!(sqllog.thread, None);
        assert_eq!(sqllog.user, None);
        assert_eq!(sqllog.trx_id, None);
        assert_eq!(sqllog.statement, None);
    }

    #[test]
    fn test_parse_segment_with_thread_minus_one() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:-1 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) TEST QUERY";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.thread, Some("-1".to_string()));
    }

    #[test]
    fn test_parse_segment_with_execute_time() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM users\nEXECTIME: 1500(ms)";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.execute_time, Some(1500));
        assert!(sqllog.description.contains("SELECT * FROM users"));
        assert!(sqllog.description.contains("EXECTIME: 1500(ms)"));
    }

    #[test]
    fn test_parse_segment_with_rowcount() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) UPDATE users SET name='test'\nEXECTIME: 500(ms) ROWCOUNT: 42";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.execute_time, Some(500));
        assert_eq!(sqllog.rowcount, Some(42));
        assert!(sqllog.description.contains("UPDATE users"));
        assert!(sqllog.description.contains("ROWCOUNT: 42"));
    }

    #[test]
    fn test_parse_segment_with_exec_id() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) INSERT INTO users\nEXECTIME: 100(ms) ROWCOUNT: 1 EXEC_ID: 98765";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.execute_time, Some(100));
        assert_eq!(sqllog.rowcount, Some(1));
        assert_eq!(sqllog.execute_id, Some(98765));
        assert!(sqllog.description.contains("INSERT INTO users"));
        assert!(sqllog.description.contains("EXEC_ID: 98765"));
    }

    #[test]
    fn test_parse_segment_with_all_numbers() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) DELETE FROM users WHERE id=1\nEXECTIME: 2000(ms) ROWCOUNT: 1 EXEC_ID: 55555";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.execute_time, Some(2000));
        assert_eq!(sqllog.rowcount, Some(1));
        assert_eq!(sqllog.execute_id, Some(55555));
    }

    #[test]
    fn test_parse_segment_multiline_description() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT users.id,\n    users.name,\n    users.email\nFROM users\nWHERE users.active = 1\nORDER BY users.created_at DESC";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert!(sqllog.description.contains("SELECT users.id,"));
        assert!(sqllog.description.contains("users.name,"));
        assert!(sqllog.description.contains("FROM users"));
        assert!(sqllog.description.contains("ORDER BY users.created_at DESC"));
    }

    #[test]
    fn test_parse_segment_invalid_format() {
        let segment = "This is not a valid SQL log format";

        let result = SqllogParser::parse_segment(segment, 1);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.is_format_error());
    }

    #[test]
    fn test_parse_segment_empty_string() {
        let segment = "";

        let result = SqllogParser::parse_segment(segment, 1);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.is_format_error());
    }

    #[test]
    fn test_parse_segment_malformed_timestamp() {
        let segment = "invalid-timestamp (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) TEST";

        let result = SqllogParser::parse_segment(segment, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_segment_missing_ep() {
        let segment = "2025-09-16 20:02:53.562 (sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) TEST";

        let result = SqllogParser::parse_segment(segment, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_segment_complex_sql_query() {
        let segment = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname:DataAnalyzer ip:::ffff:10.0.0.1) [SEL]: SELECT
    u.id,
    u.name,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= '2024-01-01'
    AND u.active = 1
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC
LIMIT 100
EXECTIME: 3500(ms) ROWCOUNT: 85 EXEC_ID: 123456"#;

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert_eq!(sqllog.occurrence_time, "2025-09-16 20:02:53.562");
        assert_eq!(sqllog.ep, "0");
        assert_eq!(sqllog.appname, Some("DataAnalyzer".to_string()));
        assert_eq!(sqllog.ip, Some("10.0.0.1".to_string()));
        assert_eq!(sqllog.sql_type, Some("SEL".to_string()));
        assert_eq!(sqllog.execute_time, Some(3500));
        assert_eq!(sqllog.rowcount, Some(85));
        assert_eq!(sqllog.execute_id, Some(123456));
        assert!(sqllog.description.contains("SELECT"));
        assert!(sqllog.description.contains("FROM users u"));
        assert!(sqllog.description.contains("LIMIT 100"));
    }

    #[test]
    fn test_parse_segment_with_special_characters() {
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) INSERT INTO messages (content) VALUES ('Hello, 世界! 🌍')";

        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());

        let sqllog = result.unwrap();
        assert!(sqllog.description.contains("Hello, 世界! 🌍"));
    }

    #[test]
    fn test_parse_segment_different_sql_types() {
        let test_cases = vec![
            ("INS", "INSERT INTO users (name) VALUES ('test')"),
            ("UPD", "UPDATE users SET name = 'updated'"),
            ("DEL", "DELETE FROM users WHERE id = 1"),
            ("SEL", "SELECT * FROM users"),
            ("ORA", "CREATE TABLE test_table (id INT)"),
        ];

        for (sql_type, query) in test_cases {
            let segment = format!(
                "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [{}]: {}",
                sql_type, query
            );

            let result = SqllogParser::parse_segment(&segment, 1).unwrap();
            assert!(result.is_some());

            let sqllog = result.unwrap();
            assert_eq!(sqllog.sql_type, Some(sql_type.to_string()));
            assert!(sqllog.description.contains(query));
        }
    }

    #[test]
    fn test_parse_segment_edge_cases() {
        // 测试边界情况

        // 极短的描述
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) X";
        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().description, "X");

        // 空的appname
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname:) TEST";
        let result = SqllogParser::parse_segment(segment, 1).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().appname, None);
    }

    #[test]
    fn test_parse_segment_number_extraction_edge_cases() {
        // 测试数字提取的边界情况

        // 只有EXECTIME
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT 1\nEXECTIME: 100(ms)";
        let result = SqllogParser::parse_segment(segment, 1).unwrap().unwrap();
        assert_eq!(result.execute_time, Some(100));
        assert_eq!(result.rowcount, None);
        assert_eq!(result.execute_id, None);

        // EXECTIME和ROWCOUNT
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT 1\nEXECTIME: 200(ms) ROWCOUNT: 5";
        let result = SqllogParser::parse_segment(segment, 1).unwrap().unwrap();
        assert_eq!(result.execute_time, Some(200));
        assert_eq!(result.rowcount, Some(5));
        assert_eq!(result.execute_id, None);

        // 所有三个数字
        let segment = "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT 1\nEXECTIME: 300(ms) ROWCOUNT: 10 EXEC_ID: 999";
        let result = SqllogParser::parse_segment(segment, 1).unwrap().unwrap();
        assert_eq!(result.execute_time, Some(300));
        assert_eq!(result.rowcount, Some(10));
        assert_eq!(result.execute_id, Some(999));
    }

    #[test]
    fn test_parse_segment_error_line_number() {
        let segment = "invalid format";
        let line_num = 42;

        let result = SqllogParser::parse_segment(segment, line_num);
        assert!(result.is_err());

        let error_msg = format!("{}", result.unwrap_err());
        assert!(error_msg.contains("42")); // 确保错误信息包含行号
    }

    #[test]
    fn test_parse_segment_real_world_examples() {
        // 基于实际日志文件的真实示例
        let real_examples = vec![
            "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)",
            "2025-09-16 20:02:53.563 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) [UPD]: UPDATE DM_BASE.DM_SEQUENCES SET CURRVAL = CURRVAL + 1 WHERE SEQNAME = 'DM_TBL_COLS_SEQNO' AND SCHNAME = 'DM_BASE'",
            "2025-09-16 20:02:53.564 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) EXECTIME: 1 ms ROWCOUNT: 1",
        ];

        for (i, example) in real_examples.iter().enumerate() {
            let result = SqllogParser::parse_segment(example, i + 1);
            assert!(result.is_ok(), "Failed to parse example {}: {}", i + 1, example);

            let sqllog = result.unwrap();
            assert!(sqllog.is_some(), "Example {} returned None", i + 1);
        }
    }
}
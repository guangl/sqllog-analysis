use sqllog_analysis::sqllog::utils::*;
use sqllog_analysis::error::SqllogError;

/// 测试闰年判断函数
#[test]
fn test_is_leap_year() {
    // 测试能被4整除但不能被100整除的年份（闰年）
    assert!(is_leap_year(2024));
    assert!(is_leap_year(2020));
    assert!(is_leap_year(2016));

    // 测试不能被4整除的年份（非闰年）
    assert!(!is_leap_year(2023));
    assert!(!is_leap_year(2021));
    assert!(!is_leap_year(2019));

    // 测试能被100整除但不能被400整除的年份（非闰年）
    assert!(!is_leap_year(1900));
    assert!(!is_leap_year(1700));
    assert!(!is_leap_year(1800));

    // 测试能被400整除的年份（闰年）
    assert!(is_leap_year(2000));
    assert!(is_leap_year(1600));
    assert!(is_leap_year(2400));

    // 边界值测试
    assert!(!is_leap_year(1));
    assert!(is_leap_year(4));
    assert!(!is_leap_year(100));
    assert!(is_leap_year(400));
}

/// 测试首行时间戳识别 - 有效格式
#[test]
fn test_is_first_row_valid() {
    // 标准有效格式
    assert!(is_first_row("2024-09-16 20:02:53.123"));
    assert!(is_first_row("2023-12-31 23:59:59.999"));
    assert!(is_first_row("2020-02-29 00:00:00.000")); // 闰年2月29日

    // 边界值测试
    assert!(is_first_row("0001-01-01 00:00:00.000"));
    assert!(is_first_row("9999-12-31 23:59:59.999"));

    // 各种有效月份
    for month in 1..=12 {
        let timestamp = format!("2024-{:02}-15 12:30:45.678", month);
        assert!(is_first_row(&timestamp));
    }
}

/// 测试首行时间戳识别 - 无效格式
#[test]
fn test_is_first_row_invalid() {
    // 长度错误
    assert!(!is_first_row("2024-09-16 20:02:53.12")); // 太短
    assert!(!is_first_row("2024-09-16 20:02:53.1234")); // 太长
    assert!(!is_first_row(""));

    // 分隔符错误
    assert!(!is_first_row("2024/09/16 20:02:53.123"));
    assert!(!is_first_row("2024-09-16T20:02:53.123"));
    assert!(!is_first_row("2024-09-16 20-02-53.123"));

    // 非数字字符
    assert!(!is_first_row("202a-09-16 20:02:53.123"));
    assert!(!is_first_row("2024-0x-16 20:02:53.123"));

    // 无效日期值
    assert!(!is_first_row("0000-09-16 20:02:53.123")); // 年份为0
    assert!(!is_first_row("2024-00-16 20:02:53.123")); // 月份为0
    assert!(!is_first_row("2024-13-16 20:02:53.123")); // 月份超过12
    assert!(!is_first_row("2024-09-00 20:02:53.123")); // 日期为0
    assert!(!is_first_row("2024-09-32 20:02:53.123")); // 日期超过31

    // 2月份特殊情况
    assert!(!is_first_row("2023-02-29 20:02:53.123")); // 非闰年2月29日
    assert!(!is_first_row("2024-02-30 20:02:53.123")); // 2月30日不存在

    // 无效时间值
    assert!(!is_first_row("2024-09-16 24:02:53.123")); // 小时超过23
    assert!(!is_first_row("2024-09-16 20:60:53.123")); // 分钟超过59
    assert!(!is_first_row("2024-09-16 20:02:60.123")); // 秒钟超过59
}

/// 测试各月天数限制
#[test]
fn test_month_days_validation() {
    // 31天的月份
    let months_31_days = [1, 3, 5, 7, 8, 10, 12];
    for month in months_31_days {
        assert!(is_first_row(&format!("2024-{:02}-31 12:00:00.000", month)));
        assert!(!is_first_row(&format!("2024-{:02}-32 12:00:00.000", month)));
    }

    // 30天的月份
    let months_30_days = [4, 6, 9, 11];
    for month in months_30_days {
        assert!(is_first_row(&format!("2024-{:02}-30 12:00:00.000", month)));
        assert!(!is_first_row(&format!("2024-{:02}-31 12:00:00.000", month)));
    }

    // 2月份测试
    assert!(is_first_row("2024-02-29 12:00:00.000")); // 闰年
    assert!(!is_first_row("2023-02-29 12:00:00.000")); // 非闰年
    assert!(is_first_row("2023-02-28 12:00:00.000")); // 非闰年最后一天
    assert!(is_first_row("2024-02-28 12:00:00.000")); // 闰年倒数第二天
}

/// 测试查找首行位置
#[test]
fn test_find_first_row_pos() {
    // 基础情况
    assert_eq!(find_first_row_pos("2024-09-16 20:02:53.123"), Some(0));

    // 在文本中间找到时间戳
    let text = "Some random text\n2024-09-16 20:02:53.123 [INFO] Message";
    assert_eq!(find_first_row_pos(text), Some(17));

    // 多个时间戳，应该返回第一个
    let text = "Before\n2024-09-16 20:02:53.123\nAfter\n2024-09-17 21:03:54.456";
    assert_eq!(find_first_row_pos(text), Some(7));

    // 没有找到
    assert_eq!(find_first_row_pos("No timestamp here"), None);
    assert_eq!(find_first_row_pos("2024-09-16 20:02:53.12"), None); // 太短

    // 空字符串和边界情况
    assert_eq!(find_first_row_pos(""), None);
    assert_eq!(find_first_row_pos("short"), None);

    // 恰好23个字符
    assert_eq!(find_first_row_pos("2024-09-16 20:02:53.123"), Some(0));
}

/// 测试字节转字符串功能 - 有效UTF-8
#[test]
fn test_line_bytes_to_str_valid_utf8() {
    let mut errors = Vec::new();

    // 标准ASCII文本
    let bytes = b"Hello, world!";
    let result = line_bytes_to_str_impl(bytes, 1, &mut errors);
    assert_eq!(result.as_ref(), "Hello, world!");
    assert!(errors.is_empty());
    assert!(matches!(result, std::borrow::Cow::Borrowed(_)));

    // UTF-8中文文本
    let bytes = "你好，世界！".as_bytes();
    let result = line_bytes_to_str_impl(bytes, 2, &mut errors);
    assert_eq!(result.as_ref(), "你好，世界！");
    assert!(errors.is_empty());
    assert!(matches!(result, std::borrow::Cow::Borrowed(_)));

    // 空字符串
    let bytes = b"";
    let result = line_bytes_to_str_impl(bytes, 3, &mut errors);
    assert_eq!(result.as_ref(), "");
    assert!(errors.is_empty());
}

/// 测试字节转字符串功能 - 无效UTF-8
#[test]
fn test_line_bytes_to_str_invalid_utf8() {
    let mut errors = Vec::new();

    // 无效UTF-8字节序列
    let invalid_bytes = &[0xFF, 0xFE, 0x48, 0x65, 0x6C, 0x6C, 0x6F];
    let result = line_bytes_to_str_impl(invalid_bytes, 5, &mut errors);

    // 应该生成错误
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 5); // 行号
    assert!(errors[0].1.contains("len=7")); // 错误信息包含长度
    assert!(matches!(errors[0].2, SqllogError::Utf8(_)));

    // 结果应该是owned字符串
    assert!(matches!(result, std::borrow::Cow::Owned(_)));

    // 清空错误列表测试另一种情况
    errors.clear();

    // 测试只有无效字节的情况
    let only_invalid = &[0xFF, 0xFE];
    let result2 = line_bytes_to_str_impl(only_invalid, 10, &mut errors);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 10);
    assert!(matches!(result2, std::borrow::Cow::Owned(_)));
}

/// 测试多次错误累积
#[test]
fn test_multiple_utf8_errors() {
    let mut errors = Vec::new();

    // 第一个无效序列
    let invalid1 = &[0xFF, 0xFE];
    let _result1 = line_bytes_to_str_impl(invalid1, 1, &mut errors);

    // 第二个无效序列
    let invalid2 = &[0x80, 0x80, 0x80];
    let _result2 = line_bytes_to_str_impl(invalid2, 5, &mut errors);

    // 应该有两个错误
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].0, 1);
    assert_eq!(errors[1].0, 5);

    // 都应该是UTF-8错误
    assert!(matches!(errors[0].2, SqllogError::Utf8(_)));
    assert!(matches!(errors[1].2, SqllogError::Utf8(_)));
}

/// 测试边界情况
#[test]
fn test_edge_cases() {
    // 测试恰好8字节的无效UTF-8
    let mut errors = Vec::new();
    let exactly_8_bytes = &[0xFF; 8];
    let _result = line_bytes_to_str_impl(exactly_8_bytes, 1, &mut errors);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("len=8"));
    assert!(!errors[0].1.contains("...")); // 不应该有省略号

    // 测试超过8字节的无效UTF-8
    errors.clear();
    let more_than_8_bytes = &[0xFF; 15];
    let _result2 = line_bytes_to_str_impl(more_than_8_bytes, 2, &mut errors);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].1.contains("len=15"));
    assert!(errors[0].1.contains("...")); // 应该有省略号
}

/// 测试时间戳格式的各种变体
#[test]
fn test_timestamp_variations() {
    // 测试所有可能的小时值
    for hour in 0..=23 {
        let timestamp = format!("2024-09-16 {:02}:30:45.123", hour);
        assert!(is_first_row(&timestamp));
    }

    // 测试所有可能的分钟值
    for minute in 0..=59 {
        let timestamp = format!("2024-09-16 12:{:02}:45.123", minute);
        assert!(is_first_row(&timestamp));
    }

    // 测试所有可能的秒值
    for second in 0..=59 {
        let timestamp = format!("2024-09-16 12:30:{:02}.123", second);
        assert!(is_first_row(&timestamp));
    }

    // 测试毫秒部分的各种值
    assert!(is_first_row("2024-09-16 12:30:45.000"));
    assert!(is_first_row("2024-09-16 12:30:45.999"));
    assert!(is_first_row("2024-09-16 12:30:45.001"));
}

/// 测试查找位置功能的边界情况
#[test]
fn test_find_first_row_pos_edge_cases() {
    // 在字符串末尾的时间戳
    let text = "prefix2024-09-16 20:02:53.123";
    assert_eq!(find_first_row_pos(text), Some(6));

    // 恰好长度为23的字符串
    let text = "2024-09-16 20:02:53.123";
    assert_eq!(find_first_row_pos(text), Some(0));

    // 长度小于23的字符串
    let text = "2024-09-16 20:02:53.12";
    assert_eq!(find_first_row_pos(text), None);

    // 包含换行符的情况
    let text = "line1\n2024-09-16 20:02:53.123\nline3";
    assert_eq!(find_first_row_pos(text), Some(6));
}
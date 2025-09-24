use sqllog_analysis::error::SqllogError;
use sqllog_analysis::sqllog::parser::SqllogParser;
use sqllog_analysis::sqllog::utils::{
    find_first_row_pos, is_first_row, is_leap_year, line_bytes_to_str_impl,
};
use std::borrow::Cow;

#[test]
fn test_is_leap_year_and_first_row() {
    assert!(is_leap_year(2024));
    assert!(!is_leap_year(1900));

    let good = "2025-09-24 12:34:56.789";
    assert!(is_first_row(good));

    let bad = "not a time";
    assert!(!is_first_row(bad));
}

#[test]
fn test_find_first_row_pos() {
    let s = "garbage\n2025-09-24 12:34:56.789 some rest";
    assert_eq!(find_first_row_pos(s), Some(8));
    assert_eq!(find_first_row_pos("short"), None);
}

#[test]
fn test_line_bytes_to_str_impl_utf8_ok_and_err() {
    let mut errors = Vec::new();
    let ok = b"hello world" as &[u8];
    match line_bytes_to_str_impl(ok, 1, &mut errors) {
        Cow::Borrowed(s) => assert_eq!(s, "hello world"),
        _ => panic!("expected borrowed"),
    }

    // invalid utf8 bytes
    let invalid = b"hello \xFF world";
    let mut errors2 = Vec::new();
    let res = line_bytes_to_str_impl(invalid, 2, &mut errors2);
    match res {
        Cow::Owned(s) => {
            assert!(s.contains("hello"));
            assert!(!errors2.is_empty());
            assert!(matches!(errors2[0].2, SqllogError::Utf8(_)));
        }
        _ => panic!("expected owned"),
    }
}

#[test]
fn test_parse_segment_and_desc_numbers_via_parse() {
    let segment = "2025-09-24 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: select 1\nEXECTIME: 123(ms) ROWCOUNT: 1 EXEC_ID: 42.";
    let res = SqllogParser::parse_segment(segment, 1).unwrap();
    assert!(res.is_some());
    let log = res.unwrap();
    assert_eq!(log.execute_time, Some(123));
    assert_eq!(log.rowcount, Some(1));
    assert_eq!(log.execute_id, Some(42));
}

// Exporter tests are skipped here because exporters are behind cargo features.

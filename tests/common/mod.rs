//! 集成测试公共模块

use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// 创建测试用的 SQL 日志文件
pub fn create_test_sqllog(
    dir: &TempDir,
    filename: &str,
    content: &str,
) -> std::path::PathBuf {
    let file_path = dir.path().join(filename);
    fs::write(&file_path, content).expect("Failed to write test file");
    file_path
}

/// 标准测试 SQL 日志内容（达梦数据库格式）
#[allow(dead_code)]
pub const SAMPLE_SQLLOG_CONTENT: &str = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705437), (1, VARCHAR2, 'CS_9244714bee58')}
2025-09-16 20:02:53.563 (EP[0] sess:0x1734a7fdc0 thrd:4146367 user:EDM_BASE trxid:0 stmt:NULL appname:) TRX: COMMIT LSN[4636972262715]
2025-09-16 20:02:53.564 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705442), (1, VARCHAR2, 'CS_51c8b99bb3f6')}
2025-09-16 20:02:53.565 (EP[0] sess:0x4fa244fe0 thrd:4135956 user:EKP trxid:0 stmt:NULL appname:) MSG: COMMIT
"#;

/// 复杂测试 SQL 日志内容（达梦数据库格式）
#[allow(dead_code)]
pub const COMPLEX_SQLLOG_CONTENT: &str = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705437), (1, VARCHAR2, 'CS_9244714bee58'), (2, VARCHAR2, NULL)}
2025-09-16 20:02:53.563 (EP[0] sess:0x1734a7fdc0 thrd:4146367 user:EDM_BASE trxid:0 stmt:NULL appname:) TRX: COMMIT LSN[4636972262715]
2025-09-16 20:02:53.564 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705442), (1, VARCHAR2, 'CS_51c8b99bb3f6')}
2025-09-16 20:02:53.565 (EP[0] sess:0x4fa244fe0 thrd:4135956 user:EKP trxid:0 stmt:NULL appname:) MSG: COMMIT
2025-09-16 20:02:53.566 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705447), (1, VARCHAR2, 'CS_2e608a3ac5c8')}
2025-09-16 20:02:53.567 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453029 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705452), (1, VARCHAR2, 'CS_abc123def456')}
2025-09-16 20:02:53.568 (EP[0] sess:0x1734a7fdc0 thrd:4146367 user:EDM_BASE trxid:0 stmt:NULL appname:) TRX: ROLLBACK LSN[4636972262716]
"#;

/// 创建多个测试文件（达梦数据库格式）
#[allow(dead_code)]
pub fn create_multiple_test_files(
    dir: &TempDir,
    count: usize,
) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();

    for i in 0..count {
        let filename = format!("test_{}.log", i + 1);
        let content = format!(
            "2025-09-16 20:02:53.{:03} (EP[0] sess:0x{:x} thrd:{} user:TEST_USER trxid:{} stmt:0x{:x} appname:) PARAMS(SEQNO, TYPE, DATA)={{(0, NUMBER, {}), (1, VARCHAR2, 'CS_test_{}')}}",
            562 + i,
            0x6da8ccef0 + i as u64,
            4146217 + i,
            122154453026 + i as u64,
            0x6da900ef0 + i as u64,
            1705437 + i,
            i
        );

        let file_path = create_test_sqllog(dir, &filename, &content);
        files.push(file_path);
    }

    files
}

/// 验证文件存在且非空
#[allow(dead_code)]
pub(crate) fn verify_output_file_exists(path: &Path) -> bool {
    path.exists() && fs::metadata(path).map(|m| m.len() > 0).unwrap_or(false)
}

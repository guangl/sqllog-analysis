use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use duckdb::Connection;
use sqllog_analysis::duckdb_writer::write_sqllogs_to_duckdb;
use sqllog_analysis::sqllog::Sqllog;
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;

fn generate_sqllogs(n: usize) -> Vec<Sqllog> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(Sqllog {
            occurrence_time: format!("2025-09-21 12:00:00.{:03}", i % 1000),
            ep: 1,
            session: Some("sess".to_string()),
            thread: Some("thrd".to_string()),
            user: Some("user".to_string()),
            trx_id: Some("trx".to_string()),
            statement: Some("SELECT 1".to_string()),
            appname: Some("app".to_string()),
            ip: Some("127.0.0.1".to_string()),
            sql_type: Some("SEL".to_string()),
            description: format!("bench description {}", i),
            execute_time: Some(10),
            rowcount: Some(1),
            execute_id: Some(i as i64),
        });
    }
    v
}

fn bench_duckdb_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("duckdb_write_modes");
    // Increase measurement_time and sample_size for more stable results
    group.measurement_time(Duration::new(30, 0));
    group.sample_size(100);
    // benchmark on multiple sizes to observe scaling behavior
    let sizes = [10_000usize, 50_000usize, 200_000usize];

    for &n in &sizes {
        let records = generate_sqllogs(n);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("appender_direct", n),
            &n,
            |b, _| {
                b.iter_batched(
                    || {
                        let tmp = NamedTempFile::new().unwrap();
                        // take ownership of the path and drop the NamedTempFile so DuckDB can open it on Windows
                        let p = tmp.path().to_path_buf();
                        drop(tmp);
                        p
                    },
                    |path| {
                        write_sqllogs_to_duckdb(&path, &records, false)
                            .unwrap();
                    },
                    criterion::BatchSize::PerIteration,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("in_memory_ctas", n),
            &n,
            |b, _| {
                b.iter_batched(
                    || {
                        let tmp = NamedTempFile::new().unwrap();
                        let p = tmp.path().to_path_buf();
                        drop(tmp);
                        p
                    },
                    |path| {
                        write_sqllogs_to_duckdb(&path, &records, true).unwrap();
                    },
                    criterion::BatchSize::PerIteration,
                )
            },
        );

        // CSV + COPY: 写到临时 CSV，然后用 DuckDB COPY FROM 导入目标 DB
        group.bench_with_input(BenchmarkId::new("csv_copy", n), &n, |b, _| {
            b.iter_batched(
                || {
                    let tmp_db = NamedTempFile::new().unwrap();
                    let tmp_csv = NamedTempFile::new().unwrap();
                    (tmp_db, tmp_csv)
                },
                |(tmp_db, tmp_csv)| {
                    // 1) 写 CSV
                    let csv_path = tmp_csv.path().to_path_buf();
                    let mut f = File::create(&csv_path).unwrap();
                    // header
                    let _ = writeln!(f, "occurrence_time,ep,session,thread,user,trx_id,statement,appname,ip,sql_type,description,execute_time,rowcount,execute_id");
                    let quote = |s: &str| {
                        let esc = s.replace("\"", "\"\"");
                        format!("\"{}\"", esc)
                    };
                    for r in &records {
                        let sess = r.session.as_deref().unwrap_or("");
                        let thread = r.thread.as_deref().unwrap_or("");
                        let user = r.user.as_deref().unwrap_or("");
                        let trx = r.trx_id.as_deref().unwrap_or("");
                        let stmt = r.statement.as_deref().unwrap_or("");
                        let app = r.appname.as_deref().unwrap_or("");
                        let ip = r.ip.as_deref().unwrap_or("");
                        let sqlt = r.sql_type.as_deref().unwrap_or("");
                        let et = r.execute_time.map(|v| v.to_string()).unwrap_or_default();
                        let rc = r.rowcount.map(|v| v.to_string()).unwrap_or_default();
                        let eid = r.execute_id.map(|v| v.to_string()).unwrap_or_default();

                        let line = format!(
                            "{occ},{ep},{sess},{thread},{user},{trx},{stmt},{app},{ip},{sqlt},{desc},{et},{rc},{eid}",
                            occ = quote(&r.occurrence_time),
                            ep = r.ep,
                            sess = quote(sess),
                            thread = quote(thread),
                            user = quote(user),
                            trx = quote(trx),
                            stmt = quote(stmt),
                            app = quote(app),
                            ip = quote(ip),
                            sqlt = quote(sqlt),
                            desc = quote(&r.description),
                            et = et,
                            rc = rc,
                            eid = eid
                        );
                        let _ = writeln!(f, "{}", line);
                    }
                    f.flush().unwrap();
                    // close the CSV file handle before DuckDB attempts to read it
                    drop(f);
                    // drop the NamedTempFile handle so DuckDB can open the db file path on Windows
                    let db_path = tmp_db.path().to_path_buf();
                    drop(tmp_db);

                    // 2) 在目标 DB 中确保表并 COPY FROM
                    let conn = Connection::open(&db_path).unwrap();
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS sqllogs (
                            occurrence_time TEXT NOT NULL,
                            ep INTEGER NOT NULL,
                            session TEXT,
                            thread TEXT,
                            user TEXT,
                            trx_id TEXT,
                            statement TEXT,
                            appname TEXT,
                            ip TEXT,
                            sql_type TEXT,
                            description TEXT NOT NULL,
                            execute_time BIGINT,
                            rowcount BIGINT,
                            execute_id BIGINT
                        )",
                        duckdb::params![],
                    ).unwrap();
                    let csv_path = csv_path.to_string_lossy().replace('"', "''");
                    let copy_sql = format!("COPY sqllogs FROM '{}' (FORMAT CSV, HEADER TRUE)", csv_path.replace('\\', "\\\\"));
                    conn.execute(&copy_sql, duckdb::params![]).unwrap();
                },
                criterion::BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, bench_duckdb_writes);
criterion_main!(benches);

//! SQLite Database Checker
//! Check the contents of the generated SQLite database

#[cfg(feature = "exporter-sqlite")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rusqlite::Connection;

    let db_path = "output/output.db";
    let conn = Connection::open(db_path)?;

    // Check table structure
    println!("=== Table Schema ===");
    let mut stmt = conn.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='sqllogs'")?;
    let rows: Vec<String> = stmt.query_map([], |row| {
        Ok(row.get::<_, String>(0)?)
    })?.collect::<Result<Vec<_>, _>>()?;

    for sql in rows {
        println!("{}", sql);
    }

    // Check row count
    println!("\n=== Row Count ===");
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM sqllogs")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("Total records: {}", count);

    // Show sample data
    println!("\n=== Sample Data ===");
    let mut stmt = conn.prepare("SELECT occurrence_time, ep, user, sql_type, description FROM sqllogs LIMIT 3")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;

    for (i, row) in rows.enumerate() {
        let (time, ep, user, sql_type, desc) = row?;
        println!("{}. {} EP[{}] {} [{}]: {}", i + 1, time, ep, user, sql_type,
                 if desc.len() > 50 { &desc[0..50] } else { &desc });
    }

    Ok(())
}

#[cfg(not(feature = "exporter-sqlite"))]
fn main() {
    println!("This example requires the 'exporter-sqlite' feature");
    println!("Run with: cargo run --features exporter-sqlite --example check_sqlite");
}
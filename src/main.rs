use anyhow::Result;

mod sqllog;

fn main() -> Result<()> {
    sqllog::is_first_row("2023-10-10 10:10:10.100");
    Ok(())
}

use std::env;

fn main() {
    // Only add this system library on Windows targets. DuckDB's bundled
    // library may reference Restart Manager (Rm*) symbols which are
    // provided by Rstrtmgr.lib on Windows. Instruct rustc to link it.
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "windows" {
        println!("cargo:rustc-link-lib=Rstrtmgr");
    }
}

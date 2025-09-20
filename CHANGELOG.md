# Changelog

All notable changes to this project will be documented in this file.

## [v0.2.0] - 2025-09-20
### Added
- DuckDB appender-based bulk writer for `Sqllog` records with configurable chunk size.
- `IndexReport` to capture per-index creation timing and errors.
- CLI options to export `IndexReport` as JSON: `--duckdb-path`, `--duckdb-report`, `--duckdb-chunk-size`.
- Integration tests for DuckDB writer and index failure injection.
- Strict CI clippy enforcement (pedantic/nursery/cargo) in GitHub Actions workflow.

### Changed
- Numeric fields in `Sqllog` migrated to signed `i64` and database schema updated (`BIGINT`).
- README and documentation updated with usage examples and CI badge.

### Fixed
- Addressed clippy pedantic warnings and other small refactors.


[Unreleased]: https://github.com/guangl/sqllog-analysis/compare/v0.1.0...v0.2.0

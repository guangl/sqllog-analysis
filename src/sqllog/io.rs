use crate::sqllog::types::{Sqllog, SqllogError};
use std::io::{self, BufRead, BufReader};

impl Sqllog {
    /// Parse the file and call `hook` when parsing completes or when a chunk boundary is reached.
    ///
    /// # Errors
    ///
    /// Returns `Err(SqllogError::Io(_))` if the input file cannot be opened or read.
    /// Parsing errors encountered during processing are reported via the `err_hook` callback
    /// and do not cause this function to return `Err`.
    pub fn parse_all<P, F, EF>(
        path: P,
        hook: F,
        err_hook: EF,
    ) -> Result<(), SqllogError>
    where
        P: AsRef<std::path::Path>,
        F: FnMut(&[Self]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        // Errors
        //
        // # Errors
        //
        // Returns `Err(SqllogError::Io(_))` if the input file cannot be opened or read.
        // Parsing errors encountered during processing are reported via the `err_hook` callback
        // and do not cause this function to return `Err`.
        Self::stream_parse(path, None, hook, err_hook)
    }

    /// 解析文件并按块处理；当解析到 `chunk_size` 条记录时会调用 `hook`，hook 返回 `true` 表示继续，返回 `false` 表示提前终止解析。
    /// Parse the file in chunks and call `hook` each time `chunk_size` records are parsed.
    ///
    /// # Errors
    ///
    /// Returns `Err(SqllogError::Io(_))` if the input file cannot be opened or read.
    /// Parsing errors encountered during processing are reported via the `err_hook` callback
    /// and do not cause this function to return `Err`.
    pub fn parse_in_chunks<P, F, EF>(
        path: P,
        chunk_size: usize,
        hook: F,
        err_hook: EF,
    ) -> Result<(), SqllogError>
    where
        P: AsRef<std::path::Path>,
        F: FnMut(&[Self]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        // Errors
        //
        // # Errors
        //
        // Returns `Err(SqllogError::Io(_))` if the input file cannot be opened or read.
        // Parsing errors encountered during processing are reported via the `err_hook` callback
        // and do not cause this function to return `Err`.
        Self::stream_parse(path, Some(chunk_size), hook, err_hook)
    }

    /// 从文件按块解析 sqllog，每当解析到 `chunk_size` 条记录或文件结尾时调用 `hook`。
    /// hook: `FnMut(&[Sqllog])` — 对每个 chunk 调用，解析会一直进行到文件结束（hook 无法中止解析）。
    /// 解析整个文件并返回所有解析出的 `Sqllog` 项与解析错误
    // 通用的流式解析器：当 chunk_size == None 时，只在 EOF 调用 hook；否则每达到 chunk_size 调用一次
    fn stream_parse<P, F, EF>(
        path: P,
        chunk_size: Option<usize>,
        mut hook: F,
        mut err_hook: EF,
    ) -> Result<(), SqllogError>
    where
        P: AsRef<std::path::Path>,
        F: FnMut(&[Self]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        // 初始化流处理状态
        let (file_name, total) = Self::init_stream_state(&path)?;
        log::trace!("开始处理文件: {file_name}");
        // 若文件为空则正常返回（不视为错误）
        if total == 0 {
            return Ok(());
        }

        let mut last_percent = 0u8;
        let mut chunk = Vec::with_capacity(chunk_size.unwrap_or(1).max(1));
        let mut chunk_errors: Vec<(usize, String, SqllogError)> = Vec::new();
        let mut has_first_row = false;

        let mut content = String::new();
        let mut line_num = 1usize;
        let mut parsed_count: usize = 0;
        let mut errors_total: usize = 0;

        // 使用 read_file_lines，并将每行处理逻辑委派给 process_line_callback
        let path_clone = path.as_ref().to_path_buf();
        let mut total_offset = 0usize;
        let file_name_clone = file_name.clone();
        // 开始计时
        let start = std::time::Instant::now();

        let mut per_line = |line: &[u8], n: usize| {
            total_offset = total_offset.saturating_add(n);
            Self::print_progress(
                total_offset,
                total,
                &mut last_percent,
                &file_name_clone,
            );
            Self::process_line_callback(
                line,
                &mut line_num,
                &mut has_first_row,
                &mut content,
                &mut chunk,
                &mut chunk_errors,
                chunk_size,
                &mut hook,
                &mut err_hook,
                &mut parsed_count,
                &mut errors_total,
            );
        };

        Self::read_file_lines(path_clone, &mut per_line)?;

        // EOF 处理：计算耗时并传入
        let elapsed = start.elapsed();
        Self::finalize_at_eof(
            &content,
            line_num,
            &mut chunk,
            &mut chunk_errors,
            &mut hook,
            &mut err_hook,
            has_first_row,
        );

        // 输出标准化完成信息
        // 格式化耗时为更可读的字符串
        let secs = elapsed.as_secs();
        let millis = elapsed.as_millis() % 1000;
        println!(
            "\n解析完成: 文件 {file_name}，解析记录约 {parsed_count} 条，错误约 {errors_total} 条，耗时 {secs}.{millis:03} 秒"
        );

        Ok(())
    }

    // 清理 chunk 缓冲的辅助函数（仅做清理，无控制流）
    fn flush_chunk<S>(
        chunk: &mut Vec<S>,
        chunk_errors: &mut Vec<(usize, String, SqllogError)>,
    ) {
        chunk.clear();
        chunk_errors.clear();
    }

    // 按字节逐行读取文件的辅助函数并调用回调
    // 回调 cb 接收已裁剪的行字节和本次读取的字节数 (n)，用于累计进度显示
    fn read_file_lines<P, C>(path: P, mut cb: C) -> Result<(), SqllogError>
    where
        P: AsRef<std::path::Path>,
        C: FnMut(&[u8], usize),
    {
        let file =
            std::fs::File::open(path.as_ref()).map_err(SqllogError::Io)?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        while {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => false,
                Ok(n) => {
                    // 获取原始行（不去除尾部 CR/LF，也不去除前导空白）
                    cb(&buf, n);
                    true
                }
            }
        } {}

        Ok(())
    }

    // 已删除 get_raw_line：直接在 read_file_lines 中使用缓冲切片。

    // 注意：流式解析使用 BufReader::read_until，因此旧的 next_raw_line_impl 已移除。
    fn handle_raw_line_impl(
        line_bytes: &[u8],
        line_num: &mut usize,
        has_first_row: &mut bool,
        content: &mut String,
        sqllogs: &mut Vec<Self>,
        errors: &mut Vec<(usize, String, SqllogError)>,
    ) {
        // 始终获取一个 String（在无效 UTF-8 情况下可能丢失信息）。UTF-8 错误会在
        // utils::line_bytes_to_str_impl 中被记录，但不会致命；解析会继续处理后续行。
        let line_str = crate::sqllog::utils::line_bytes_to_str_impl(
            line_bytes, *line_num, errors,
        );

        Self::process_line(
            line_str.as_ref(),
            has_first_row,
            content,
            line_num,
            sqllogs,
            errors,
        );
    }

    pub fn print_progress(
        current: usize,
        total: usize,
        last_percent: &mut u8,
        file_name: &str,
    ) {
        // 使用整数运算以避免浮点转换导致的精度损失并触发 clippy 的严格警告。
        // 先以基点（basis points）计算百分比，然后除以得到整型百分比值。
        if total == 0 {
            return;
        }
        let current_u128 = current as u128;
        let total_u128 = total as u128;
        let percent_u128 = (current_u128.saturating_mul(100u128)) / total_u128;
        // 安全地转换为 u8；若值超出范围，则钳制为 100%。
        let percent = u8::try_from(percent_u128).unwrap_or(100u8);
        if percent >= last_percent.saturating_add(5) {
            print!("\r文件 {file_name} 处理进度: {percent}% ");
            io::Write::flush(&mut io::stdout()).ok();
            *last_percent = percent;
        }
    }

    // 初始化流解析所需的状态（文件名和总字节数）
    fn init_stream_state<P: AsRef<std::path::Path>>(
        path: &P,
    ) -> Result<(String, usize), SqllogError> {
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        // 尝试打开文件以区分“文件不存在”与“空文件”。
        let file =
            std::fs::File::open(path.as_ref()).map_err(SqllogError::Io)?;
        let total = file
            .metadata()
            .map(|m| usize::try_from(m.len()).unwrap_or(0usize))
            .unwrap_or(0usize);

        Ok((file_name, total))
    }

    // 每行的处理逻辑（封装为函数以便 stream_parse 更简洁）
    #[allow(clippy::too_many_arguments)]
    fn process_line_callback<F, EF>(
        line: &[u8],
        line_num: &mut usize,
        has_first_row: &mut bool,
        content: &mut String,
        chunk: &mut Vec<Self>,
        chunk_errors: &mut Vec<(usize, String, SqllogError)>,
        chunk_size: Option<usize>,
        hook: &mut F,
        err_hook: &mut EF,
        parsed_count: &mut usize,
        errors_total: &mut usize,
    ) where
        F: FnMut(&[Self]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        Self::handle_raw_line_impl(
            line,
            line_num,
            has_first_row,
            content,
            chunk,
            chunk_errors,
        );

        // update counters based on what was pushed to sqllogs/errors in process_line_impl
        // parsed_count 增长为 chunk 的当前长度之和（这是近似统计，精确计数需要在 process_line_impl 内返回信息）
        *parsed_count = parsed_count.saturating_add(chunk.len());
        *errors_total = errors_total.saturating_add(chunk_errors.len());

        if let Some(n) = chunk_size {
            if chunk.len() >= n {
                if !chunk_errors.is_empty() {
                    (err_hook)(&*chunk_errors);
                }

                if !chunk.is_empty() {
                    hook(&*chunk);
                }

                Self::flush_chunk(chunk, chunk_errors);
            }
        }
    }

    // EOF 时的最终化逻辑（flush content, report errors, call hook，然后清理）
    #[allow(clippy::too_many_arguments)]
    fn finalize_at_eof<F, EF>(
        content: &str,
        line_num: usize,
        chunk: &mut Vec<Self>,
        chunk_errors: &mut Vec<(usize, String, SqllogError)>,
        hook: &mut F,
        err_hook: &mut EF,
        has_first_row: bool,
    ) where
        F: FnMut(&[Self]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        if !content.is_empty() {
            Self::flush_content(content, line_num, chunk, chunk_errors);
        }

        // 如果从未遇到过首行，视为 "无有效日志行" 错误（与旧逻辑兼容）
        // 无论是否产生过具体的解析错误，都返回一个统一的错误，便于上层处理。
        if !has_first_row {
            // 如果存在关键错误类型（例如 UTF8/IO/Regex/ParseInt），优先返回这些错误以保留原始问题信息；
            // 否则将其归一化为 "无有效日志行"，与历史行为一致。
            let has_critical = chunk_errors.iter().any(|(_, _, e)| {
                matches!(
                    e,
                    SqllogError::Utf8(_)
                        | SqllogError::Io(_)
                        | SqllogError::Regex(_)
                        | SqllogError::ParseInt(_)
                )
            });

            if has_critical {
                (err_hook)(&*chunk_errors);
                return;
            }

            let err = SqllogError::Other("无有效日志行".to_string());
            (err_hook)(&[(0usize, "无有效日志行".to_string(), err)]);
            return;
        }

        if !chunk_errors.is_empty() {
            (err_hook)(&*chunk_errors);
        }

        if !chunk.is_empty() {
            hook(&*chunk);
        }

        Self::flush_chunk(chunk, chunk_errors);
    }
}

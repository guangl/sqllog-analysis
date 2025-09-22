use crate::sqllog::{
    types::{Sqllog, SqllogError},
    utils,
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

impl Sqllog {
    /// 解析整个文件，并在解析出记录时通过 `hook` 回调发送记录片段。
    ///
    /// 注意：此函数现在与 `parse_in_chunks` 统一签名，当 `chunk_size` 为 0 时表示不分块解析。
    ///
    /// 参数说明：
    /// - `path`: 要解析的文件路径。
    /// - `chunk_size`: 每次回调时包含的最大记录数，0 表示不分块（一次性处理所有记录）。
    /// - `hook`: 当解析出一组记录时被调用，接收记录切片 `&[Sqllog]`。
    /// - `err_hook`: 当解析过程中遇到错误时被调用，接收错误列表 `&[(usize, String, SqllogError)]`。
    ///
    /// # Errors
    /// - `SqllogError::Io(_)` - 文件打开或读取时发生 I/O 错误
    ///
    /// 返回值：
    /// - `Ok(())` 表示成功完成文件解析（解析错误通过 `err_hook` 上报，不会作为返回错误）。
    /// - `Err(SqllogError::Io(_))` 表示在打开或读取文件时发生 I/O 错误。
    pub fn parse_all<P, F, EF>(
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
        // chunk_size 为 0 时表示不分块，传递 None 给 stream_parse
        let chunk_opt = if chunk_size == 0 { None } else { Some(chunk_size) };
        Self::stream_parse(path, chunk_opt, hook, err_hook)
    }

    /// 按块解析文件，每次最多 `chunk_size` 条记录，并在每个块解析完成后调用 `hook`。
    ///
    /// 参数说明：
    /// - `path`: 要解析的文件路径。
    /// - `chunk_size`: 每次回调时包含的最大记录数。
    /// - `hook`: 当收集到一块记录时被调用，接收记录切片 `&[Sqllog]`。
    /// - `err_hook`: 当解析过程中遇到错误时被调用，接收错误列表 `&[(usize, String, SqllogError)]`。
    ///
    /// # Errors
    /// - `SqllogError::Io(_)` - 文件打开或读取时发生 I/O 错误
    ///
    /// 返回值与 `parse_all` 类似：I/O 错误会以 `Err(SqllogError::Io(_))` 返回，解析错误通过 `err_hook` 上报。
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
        Self::stream_parse(path, Some(chunk_size), hook, err_hook)
    }

    /// 流式解析实现（内部使用）。
    ///
    /// 该函数按行读取文件并逐行解析，内部维护解析状态并在必要时调用 `hook` 或 `err_hook`。
    ///
    /// 参数说明：
    /// - `path`: 要解析的文件路径。
    /// - `chunk_size`: 可选的块大小，若为 `Some(n)` 则在每 `n` 条记录时触发一次 `hook`。
    /// - `hook`: 成功解析记录时的回调，接收记录切片 `&[Sqllog]`。
    /// - `err_hook`: 解析发生错误时的回调，接收错误列表 `&[(usize, String, SqllogError)]`。
    ///
    /// 返回值：
    /// - `Ok(())` 表示解析流程完成（解析错误会通过 `err_hook` 报告而不作为返回错误）。
    /// - `Err(SqllogError::Io(_))` 表示在打开或读取文件时发生 I/O 错误。
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
        let path_ref = path.as_ref();
        log::debug!(
            "stream_parse: 开始解析文件 {}, chunk_size = {:?}",
            path_ref.display(),
            chunk_size
        );

        let (file_name, total) = Self::init_stream_state(&path)?;
        log::debug!("stream_parse: 文件大小 {total} 字节");
        log::trace!("开始处理文件: {file_name}");

        if total == 0 {
            log::debug!("stream_parse: 文件为空，直接返回");
            return Ok(());
        }

        let mut state = ParseState::new(chunk_size);

        let path_clone = path.as_ref().to_path_buf();

        let mut line_count = 0u64;
        let mut last_progress_report = std::time::Instant::now();

        // 每读取一行字节后调用的闭包，会把字节传给 ParseState 进行处理
        let mut per_line = |line: &[u8]| {
            line_count += 1;

            // 每处理 100000 行或每 5 秒报告一次进度
            if line_count % 100_000 == 0
                || last_progress_report.elapsed().as_secs() >= 5
            {
                log::debug!(
                    "stream_parse: 已处理 {} 行，解析记录数: {}",
                    line_count,
                    state.chunk.len()
                );
                last_progress_report = std::time::Instant::now();
            }

            state.process_line_callback(line, &mut hook, &mut err_hook);
        };

        log::debug!("stream_parse: 开始逐行读取文件");
        Self::read_file_lines(path_clone, &mut per_line)?;

        if !state.content.is_empty() {
            Self::flush_content(
                &state.content,
                state.line_num,
                &mut state.chunk,
                &mut state.chunk_errors,
            );
        }

        // 如果从未遇到过首行，特殊处理并返回，避免重复上报同一错误。
        if !state.has_first_row {
            let has_critical = state.chunk_errors.iter().any(|(_, _, e)| {
                matches!(
                    e,
                    SqllogError::Utf8(_)
                        | SqllogError::Io(_)
                        | SqllogError::Regex(_)
                        | SqllogError::ParseInt(_)
                )
            });

            if has_critical {
                err_hook(&state.chunk_errors);
            } else {
                let err = SqllogError::Other("无有效日志行".to_string());
                err_hook(&[(0usize, "无有效日志行".to_string(), err)]);
            }

            return Ok(());
        }

        state.finalize_at_eof(&mut hook, &mut err_hook);

        Ok(())
    }

    /// 以行为单位读取文件，并将每行字节（包含换行符）传递给 `cb` 回调。
    ///
    /// 参数说明：
    /// - `path`: 要读取的文件路径。
    /// - `cb`: 接收裁剪后的行字节切片 `&[u8]` 的回调。
    ///
    /// 返回：当无法打开或读取文件时返回 `SqllogError::Io`。
    fn read_file_lines<P, C>(path: P, mut cb: C) -> Result<(), SqllogError>
    where
        P: AsRef<std::path::Path>,
        C: FnMut(&[u8]),
    {
        let file = File::open(path.as_ref()).map_err(SqllogError::Io)?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) => break,
                Err(e) => return Err(SqllogError::Io(e)),
                Ok(_) => cb(&buf),
            }
        }

        Ok(())
    }

    /// 处理原始行字节并将其转换为字符串后交给 `process_line` 解析。
    ///
    /// 该函数负责将可能包含无效 UTF-8 的字节安全地转换为 String，
    /// 并在转换过程中将 UTF-8 错误记录到 `errors` 中，但不会中断解析流程。
    ///
    /// 参数说明：
    /// - `line_bytes`: 当前读取到的行字节（包含换行符）。
    /// - `line_num`: 当前行号引用（会在必要时更新）。
    /// - `has_first_row`: 指示是否已遇到首行（用于跳过文件头或无效内容）。
    /// - `content`: 解析时用于拼接多行记录的临时字符串缓冲。
    /// - `sqllogs`: 当前块的解析结果向量，会把解析出的记录 push 到该向量中。
    /// - `errors`: 解析过程中收集的错误列表，包含行号、原始文本片段和错误类型。
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
        let line_str =
            utils::line_bytes_to_str_impl(line_bytes, *line_num, errors);

        Self::process_line(
            line_str.as_ref(),
            has_first_row,
            content,
            line_num,
            sqllogs,
            errors,
        );
    }

    /// 初始化流解析所需的状态：返回文件名和文件总字节数。
    ///
    /// 说明：打开文件并读取 metadata 以判断文件是否存在或为空。
    ///
    /// 返回：`Ok((file_name, total_bytes))`，在无法打开文件时返回 `SqllogError::Io`。
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
        let file = File::open(path.as_ref()).map_err(SqllogError::Io)?;
        let total = file
            .metadata()
            .map(|m| usize::try_from(m.len()).unwrap_or(0usize))
            .unwrap_or(0usize);

        Ok((file_name, total))
    }
}

/// `ParseState`: 聚合解析过程的可变状态，避免函数参数过多。
///
/// 该结构保存了流式解析过程中需要的可变信息：当前行号、是否已遇到首条有效日志、
/// 当前拼接内容缓冲、当前块的解析结果与错误集合以及可选的块大小设置。
struct ParseState {
    line_num: usize,
    has_first_row: bool,
    content: String,
    chunk: Vec<Sqllog>,
    chunk_errors: Vec<(usize, String, SqllogError)>,
    chunk_size: Option<usize>,
}

impl ParseState {
    fn new(chunk_size: Option<usize>) -> Self {
        Self {
            line_num: 1usize,
            has_first_row: false,
            content: String::new(),
            chunk: Vec::with_capacity(chunk_size.unwrap_or(1).max(1)),
            chunk_errors: Vec::new(),
            chunk_size,
        }
    }

    /// 处理读取到的一行字节，将其解析并可能触发 `hook` 或 `err_hook`。
    ///
    /// 参数说明：
    /// - `line`: 当前读取到的行字节切片。
    /// - `hook`: 成功解析记录时的回调，会在满足块大小或 EOF 时被调用。
    /// - `err_hook`: 解析发生错误时的回调，会在发生错误时被调用。
    fn process_line_callback<F, EF>(
        &mut self,
        line: &[u8],
        hook: &mut F,
        err_hook: &mut EF,
    ) where
        F: FnMut(&[Sqllog]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        Sqllog::handle_raw_line_impl(
            line,
            &mut self.line_num,
            &mut self.has_first_row,
            &mut self.content,
            &mut self.chunk,
            &mut self.chunk_errors,
        );

        // 若配置了 chunk_size 且达到阈值，则触发一次块终结与回调
        if let Some(n) = self.chunk_size {
            if self.chunk.len() >= n {
                self.finalize_at_eof(hook, err_hook);
            }
        }
    }

    /// 在 EOF 或块边界处进行终结处理：上报错误并将当前块发送给 `hook`，然后清理状态。
    ///
    /// 参数说明：
    /// - `hook`: 当存在解析出的记录块时被调用以传递这些记录。
    /// - `err_hook`: 当存在收集到的解析错误时被调用以传递这些错误。
    fn finalize_at_eof<F, EF>(&mut self, hook: &mut F, err_hook: &mut EF)
    where
        F: FnMut(&[Sqllog]),
        EF: FnMut(&[(usize, String, SqllogError)]),
    {
        if !self.chunk_errors.is_empty() {
            err_hook(&self.chunk_errors);
        }

        if !self.chunk.is_empty() {
            hook(&self.chunk);
        }

        self.chunk.clear();
        self.chunk_errors.clear();
    }
}

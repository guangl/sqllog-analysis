//! 导出统计信息模块

/// 导出统计信息
#[derive(Debug, Default, Clone)]
pub struct ExportStats {
    /// 已导出的记录数
    pub exported_records: usize,
    /// 导出失败的记录数
    pub failed_records: usize,
    /// 导出开始时间
    pub start_time: Option<std::time::Instant>,
    /// 导出完成时间
    pub end_time: Option<std::time::Instant>,
}

impl ExportStats {
    /// 创建新的统计信息，记录开始时间
    pub fn new() -> Self {
        Self {
            start_time: Some(std::time::Instant::now()),
            ..Default::default()
        }
    }

    /// 标记导出完成，记录结束时间
    pub fn finish(&mut self) {
        self.end_time = Some(std::time::Instant::now());
    }

    /// 计算导出持续时间
    pub fn duration(&self) -> Option<std::time::Duration> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    /// 计算每秒导出记录数
    pub fn records_per_second(&self) -> Option<f64> {
        self.duration().map(|d| {
            if d.as_secs_f64() > 0.0 {
                self.exported_records as f64 / d.as_secs_f64()
            } else {
                0.0
            }
        })
    }

    /// 计算成功率
    pub fn success_rate(&self) -> f64 {
        let total = self.exported_records + self.failed_records;
        if total > 0 {
            self.exported_records as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    /// 获取总记录数
    pub fn total_records(&self) -> usize {
        self.exported_records + self.failed_records
    }

    /// 重置统计信息
    pub fn reset(&mut self) {
        self.exported_records = 0;
        self.failed_records = 0;
        self.start_time = Some(std::time::Instant::now());
        self.end_time = None;
    }

    /// 合并其他统计信息
    pub fn merge(&mut self, other: &ExportStats) {
        self.exported_records += other.exported_records;
        self.failed_records += other.failed_records;

        // 保持最早的开始时间
        if let Some(other_start) = other.start_time {
            if let Some(self_start) = self.start_time {
                if other_start < self_start {
                    self.start_time = Some(other_start);
                }
            } else {
                self.start_time = Some(other_start);
            }
        }

        // 保持最晚的结束时间
        if let Some(other_end) = other.end_time {
            if let Some(self_end) = self.end_time {
                if other_end > self_end {
                    self.end_time = Some(other_end);
                }
            } else {
                self.end_time = Some(other_end);
            }
        }
    }
}

impl std::fmt::Display for ExportStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "成功: {}, 失败: {}", self.exported_records, self.failed_records)?;

        if let Some(duration) = self.duration() {
            write!(f, ", 耗时: {:.2}s", duration.as_secs_f64())?;

            if let Some(rps) = self.records_per_second() {
                write!(f, ", 速度: {:.2} 记录/秒", rps)?;
            }
        }

        write!(f, ", 成功率: {:.1}%", self.success_rate())
    }
}
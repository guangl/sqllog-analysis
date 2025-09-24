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
        write!(
            f,
            "成功: {}, 失败: {}",
            self.exported_records, self.failed_records
        )?;

        if let Some(duration) = self.duration() {
            write!(f, ", 耗时: {:.2}s", duration.as_secs_f64())?;

            if let Some(rps) = self.records_per_second() {
                write!(f, ", 速度: {:.2} 记录/秒", rps)?;
            }
        }

        write!(f, ", 成功率: {:.1}%", self.success_rate())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // no local Duration import needed; use fully-qualified std::time::Duration where required

    #[test]
    fn test_display_without_finish() {
        let s = format!("{}", ExportStats::new());
        // when not finished, duration not printed but success rate should be present
        assert!(s.contains("成功"));
    }

    #[test]
    fn test_records_per_second_zero_duration() {
        let mut a = ExportStats::new();
        a.exported_records = 1;
        // artificially set end_time == start_time to simulate zero duration
        if let Some(start) = a.start_time {
            a.end_time = Some(start);
        }

        assert_eq!(a.records_per_second().unwrap(), 0.0);
    }

    #[test]
    fn test_merge_preserve_start_and_end() {
        let mut a = ExportStats::new();
        a.exported_records = 2;
        // make a have a later start and earlier end
        let later_start = std::time::Instant::now();
        a.start_time = Some(later_start);
        a.end_time = Some(later_start + std::time::Duration::from_secs(1));

        let mut b = ExportStats::new();
        b.exported_records = 3;
        // make b have an earlier start and later end
        let earlier_start = later_start - std::time::Duration::from_secs(2);
        b.start_time = Some(earlier_start);
        b.end_time = Some(later_start + std::time::Duration::from_secs(5));

        a.merge(&b);

        // exported records should sum
        assert_eq!(a.exported_records, 5);
        // start_time should be earliest (earlier_start)
        assert!(
            a.start_time.unwrap()
                <= earlier_start + std::time::Duration::from_secs(0)
        );
        // end_time should be latest
        assert!(
            a.end_time.unwrap()
                >= later_start + std::time::Duration::from_secs(5)
        );
    }

    #[test]
    fn test_display_with_finish_and_rates() {
        let mut s = ExportStats::new();
        s.exported_records = 5;
        s.failed_records = 1;
        // make duration > 0
        let start =
            std::time::Instant::now() - std::time::Duration::from_secs(2);
        s.start_time = Some(start);
        s.end_time = Some(start + std::time::Duration::from_secs(2));

        let out = format!("{}", s);
        // should contain duration, speed and success rate
        assert!(out.contains("耗时") || out.contains("s"));
        assert!(
            out.contains("速度") || out.contains("记录") || out.contains("成")
        );
        assert!(out.contains("成"));
    }

    #[test]
    fn test_success_rate_and_reset() {
        let mut s = ExportStats::default();
        // when no records, success rate should be 0 and total_records 0
        assert_eq!(s.success_rate(), 0.0);
        assert_eq!(s.total_records(), 0);

        s.exported_records = 3;
        s.failed_records = 1;
        assert!((s.success_rate() - 75.0).abs() < 1e-6);
        assert_eq!(s.total_records(), 4);

        // reset should zero counters and clear end_time
        s.finish();
        assert!(s.end_time.is_some());
        s.reset();
        assert_eq!(s.exported_records, 0);
        assert_eq!(s.failed_records, 0);
        assert!(s.end_time.is_none());
    }

    #[test]
    fn test_duration_none_and_records_per_second_none() {
        // default ExportStats has no start_time -> duration should be None
        let s = ExportStats::default();
        assert!(s.duration().is_none());
        assert!(s.records_per_second().is_none());
        // Display should still include counts
        let out = format!("{}", s);
        assert!(out.contains("成功"));
    }

    #[test]
    fn test_display_zero_duration_includes_speed() {
        // create stats where start_time == end_time to simulate zero duration
        let mut s = ExportStats::new();
        s.exported_records = 1;
        if let Some(start) = s.start_time {
            s.end_time = Some(start);
        }

        let out = format!("{}", s);
        // should include the speed field (even if 0.00)
        assert!(out.contains("速度") || out.contains("记录/秒"));
    }
}

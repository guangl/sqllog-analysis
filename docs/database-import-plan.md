# 数据库导入功能开发计划

## 📋 开发目标

实现完整的 DuckDB 数据库导入功能，使 sqllog-analysis 工具能够：
1. 将解析的 sqllog 数据存储到 DuckDB 数据库
2. 支持内存和磁盘两种数据库模式
3. 提供高性能的批量插入功能
4. 支持多种数据导出格式

## 🎯 功能规划

### Phase 1: 数据库连接和表结构
- [x] 创建 `DatabaseManager` 基础结构
- [ ] 实现数据库连接管理（内存/磁盘模式）
- [ ] 设计和创建 `sqllogs` 表结构
- [ ] 添加索引优化查询性能
- [ ] 编写单元测试

### Phase 2: 数据插入功能
- [ ] 实现 DuckDB Appender API 集成
- [ ] 开发批量插入功能
- [ ] 添加事务管理和错误处理
- [ ] 性能优化和内存管理
- [ ] 集成测试和基准测试

### Phase 3: 应用集成
- [ ] 修改 `app.rs` 集成数据库写入
- [ ] 在解析回调中添加数据库插入逻辑
- [ ] 处理解析和插入的错误协调
- [ ] 更新配置选项和验证

### Phase 4: 数据导出功能
- [ ] 实现 CSV 导出功能
- [ ] 实现 JSON 导出功能
- [ ] 实现 Excel 导出功能（可选）
- [ ] 添加导出配置和文件管理
- [ ] 多线程导出优化

## 🔧 技术实现细节

### 数据库表结构设计

```sql
CREATE TABLE IF NOT EXISTS sqllogs (
    -- 基础字段
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 会话和事务信息
    session_id BIGINT,
    transaction_id BIGINT,
    
    -- SQL 信息
    sql_text TEXT NOT NULL,
    sql_hash VARCHAR(64), -- SQL 文本的哈希值用于去重和分组
    
    -- 客户端信息
    app_name VARCHAR(255),
    client_ip INET,
    
    -- 性能指标
    execution_time_ms BIGINT,
    rows_affected BIGINT,
    
    -- 元数据
    source_file VARCHAR(500),
    source_line_number BIGINT,
    
    -- 索引
    INDEX idx_timestamp (timestamp),
    INDEX idx_session_id (session_id),
    INDEX idx_sql_hash (sql_hash),
    INDEX idx_app_name (app_name)
);
```

### 批量插入策略

1. **分块处理**：将大量数据分成小批次处理，避免内存溢出
2. **Appender API**：使用 DuckDB 的原生 Appender 获得最佳性能
3. **事务管理**：适当的事务边界确保数据一致性
4. **错误恢复**：失败重试和部分成功处理

### 性能优化考虑

1. **并发插入**：多线程并行插入不同文件的数据
2. **内存管理**：控制内存使用，避免大量数据堆积
3. **索引策略**：先插入数据再创建索引，提高插入性能
4. **压缩优化**：利用 DuckDB 的列式存储优势

## 📊 配置扩展

需要在 `config.toml` 中添加的新配置：

```toml
[database]
# 数据库文件路径
db_path = "sqllogs.duckdb"
# 是否使用内存数据库
use_in_memory = false
# 批量插入的批次大小
batch_size = 1000
# 是否启用自动索引创建
auto_create_indexes = true

[export]
# 是否启用导出功能
enabled = false
# 导出格式：csv/json/excel
format = "csv"
# 导出文件路径
out_path = "exports/sqllogs.csv"
# 是否按线程分别导出
per_thread_out = false
# 单个文件大小限制（字节）
file_size_bytes = 104857600  # 100MB
```

## 🧪 测试策略

### 单元测试
- 数据库连接管理测试
- 表结构创建和验证测试
- 批量插入功能测试
- 错误处理和边界情况测试

### 集成测试
- 端到端数据处理流程测试
- 多线程并发插入测试
- 大数据量性能测试
- 内存和磁盘模式对比测试

### 基准测试
- 插入性能基准
- 查询性能基准
- 内存使用基准
- 与其他存储方案对比

## 📅 开发时间线

- **Week 1**: Phase 1 - 数据库连接和表结构
- **Week 2**: Phase 2 - 数据插入功能
- **Week 3**: Phase 3 - 应用集成
- **Week 4**: Phase 4 - 数据导出功能
- **Week 5**: 测试、优化和文档完善

## 🔍 验收标准

1. **功能完整性**：所有规划功能正常工作
2. **性能要求**：能处理大量数据（100万+ 记录）且性能合理
3. **稳定性**：错误处理完善，无内存泄漏
4. **易用性**：配置简单，文档齐全
5. **测试覆盖**：核心功能测试覆盖率 > 80%

## 📖 相关文档

- [DuckDB Rust API 文档](https://docs.rs/duckdb/)
- [DuckDB SQL 参考](https://duckdb.org/docs/sql/introduction)
- [现有代码架构文档](../README.md)
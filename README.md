# mysqldiff

Golang 针对 MySQL 数据库表结构的差异 SQL 工具。

## 比对选项

- [x] 比对表
    - [x] 比对主键
    - [x] 比对外键（默认关闭，需要加 --foreign 参数）
    - [x] 比对索引
    - [ ] 比对触发器
    - [x] 比对字符集
    - [ ] 比对自动递增值
    - [ ] 比对分区
    - [x] 比对表选项
    - [x] 比对注释（默认关闭，需要加 --comment 参数）
- [x] 比对视图
- [ ] 比对函数
- [ ] 比对事件
- [ ] 比对定义者

## 使用

```bash
# 查看帮助
./mysqldiff --help
# 实例
./mysqldiff --source user:password@host:port --db db1:db2
./mysqldiff --source user:password@host:port --target user:password@host:port --db db1:db2
./mysqldiff --source user:password@host:port --target user:password@host:port --db db1:db2 --comment
```

## 自动补全

```bash
mysqldiff completion bash > /etc/bash_completion.d/mysqldiff && source /etc/bash_completion.d/mysqldiff
```

## 打包

> 包含命令自动补全

```bash
./install.bash
```

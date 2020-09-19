# mysqldiff

Golang 针对 MySQL 数据库表结构的差异 SQL 工具。

## 使用

```bash
# 查看帮助
./mysqldiff --help
# 实例
./mysqldiff --source user:password@host:port --db db1:db2
./mysqldiff --source user:password@host:port --target user:password@host:port --db db1:db2
```

## 打包

```bash
./install.bash
```


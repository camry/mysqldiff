package cmd

import (
    "fmt"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "sync"
    
    "github.com/spf13/cobra"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

const (
    Dsn         = "%s:%s@tcp(%s:%d)/information_schema?timeout=10s&parseTime=true&charset=%s"
    HostPattern = "^(.*)\\:(.*)\\@(.*)\\:(\\d+)$"
    DbPattern   = "^([A-Za-z0-9_]+)\\:([A-Za-z0-9_]+)$"
)

func Execute() error {
    return rootCmd.Execute()
}

func init() {
    cobra.OnInitialize(initConfig)

    rootCmd.Flags().StringVarP(&source, "source", "s", "", "指定源服务器。(格式: <user>:<password>@<host>:<port>)")
    rootCmd.Flags().StringVarP(&target, "target", "t", "", "指定目标服务器。(格式: <user>:<password>@<host>:<port>)")
    rootCmd.Flags().StringVarP(&db, "db", "d", "", "指定数据库。(格式: <source_db>:<target_db>)")
    rootCmd.Flags().BoolVarP(&comment, "comment", "c", false, "是否比对注释？")
    rootCmd.Flags().BoolVarP(&foreign, "foreign", "f", false, "是否比对外键？")
    rootCmd.Flags().BoolVarP(&tidb, "tidb", "i", false, "是否 TiDB ？")

    // cobra.CheckErr(rootCmd.MarkFlagRequired("source"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("db"))

    rootCmd.AddCommand(completionCmd)
}

func initConfig() {
}

var (
    wg   sync.WaitGroup
    lock sync.Mutex
    ch   = make(chan bool, 16)

    source  string
    target  string
    db      string
    comment bool
    foreign bool
    tidb    bool

    diffSqlKeys []string
    diffSqlMap  = make(map[string]string)

    rootCmd = &cobra.Command{
        Use:     "mysqldiff",
        Short:   "针对 MySQL 差异 SQL 工具。",
        Version: "v3.0.9",
        Run: func(cmd *cobra.Command, args []string) {
            if source == "" {
                source = os.Getenv("MYSQLDIFF_SOURCE")
            }
            if target == "" {
                target = os.Getenv("MYSQLDIFF_TARGET")
            }

            sourceMatched, err1 := regexp.MatchString(HostPattern, source)
            dbMatched, err3 := regexp.MatchString(DbPattern, db)

            cobra.CheckErr(err1)
            cobra.CheckErr(err3)

            if !sourceMatched {
                cobra.CheckErr(fmt.Errorf("源服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", source))
            }

            if !dbMatched {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 格式错误。(正确格式: <source_db>:<target_db>)", db))
            }

            var (
                sourceUser = strings.Split(source[0:strings.LastIndex(source, "@")], ":")
                sourceHost = strings.Split(source[strings.LastIndex(source, "@")+1:], ":")
                databases  = strings.Split(db, ":")

                err error
            )

            sourceDbConfig := DbConfig{
                User:     sourceUser[0],
                Password: sourceUser[1],
                Host:     sourceHost[0],
                Charset:  "utf8",
                Database: databases[0],
            }
            sourceDbConfig.Port, err = strconv.Atoi(sourceHost[1])

            cobra.CheckErr(err)

            targetDbConfig := DbConfig{
                Charset:  "utf8",
                Database: databases[1],
            }

            sourceDb, err := gorm.Open(mysql.New(mysql.Config{
                DSN: fmt.Sprintf(Dsn,
                    sourceDbConfig.User, sourceDbConfig.Password,
                    sourceDbConfig.Host, sourceDbConfig.Port,
                    sourceDbConfig.Charset,
                ),
            }), &gorm.Config{
                SkipDefaultTransaction: true,
                DisableAutomaticPing:   true,
                Logger:                 logger.Default.LogMode(logger.Silent),
            })

            cobra.CheckErr(err)

            var targetDb = sourceDb

            if target != "" {
                targetMatched, err2 := regexp.MatchString(HostPattern, target)

                cobra.CheckErr(err2)

                if !targetMatched {
                    cobra.CheckErr(fmt.Errorf("目标服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", target))
                }

                var targetUser = strings.Split(target[0:strings.LastIndex(target, "@")], ":")
                var targetHost = strings.Split(target[strings.LastIndex(target, "@")+1:], ":")

                targetDbConfig.User = targetUser[0]
                targetDbConfig.Password = targetUser[1]
                targetDbConfig.Host = targetHost[0]
                targetDbConfig.Port, err = strconv.Atoi(targetHost[1])

                cobra.CheckErr(err)

                targetDb, err = gorm.Open(mysql.New(mysql.Config{
                    DSN: fmt.Sprintf(Dsn,
                        targetDbConfig.User, targetDbConfig.Password,
                        targetDbConfig.Host, targetDbConfig.Port,
                        targetDbConfig.Charset,
                    ),
                }), &gorm.Config{
                    SkipDefaultTransaction: true,
                    DisableAutomaticPing:   true,
                    Logger:                 logger.Default.LogMode(logger.Silent),
                })

                cobra.CheckErr(err)
            }

            var (
                sourceSchema Schema
                targetSchema Schema
            )

            sourceSchemaResult := sourceDb.Table("SCHEMATA").Limit(1).Find(
                &sourceSchema,
                "`SCHEMA_NAME` = ?", sourceDbConfig.Database,
            )

            targetSchemaResult := targetDb.Table("SCHEMATA").Limit(1).Find(
                &targetSchema,
                "`SCHEMA_NAME` = ?", targetDbConfig.Database,
            )

            if sourceSchemaResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("源数据库 `%s` 不存在。", databases[0]))
            }

            if targetSchemaResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("目标数据库 `%s` 不存在。", databases[1]))
            }

            var (
                sourceTableData []Table
                targetTableData []Table
            )

            sourceDb.Table("TABLES").Order("`TABLE_NAME` ASC").Find(
                &sourceTableData,
                "`TABLE_SCHEMA` = ?", sourceDbConfig.Database,
            )
            targetDb.Table("TABLES").Order("`TABLE_NAME` ASC").Find(
                &targetTableData,
                "`TABLE_SCHEMA` = ?", targetDbConfig.Database,
            )

            sourceTableMap := make(map[string]Table)
            targetTableMap := make(map[string]Table)

            for _, table := range sourceTableData {
                sourceTableMap[table.TableName] = table
            }

            for _, table := range targetTableData {
                targetTableMap[table.TableName] = table
            }

            // DROP TABLE Or DROP VIEW...
            drop(sourceTableMap, targetTableData)

            defer close(ch)

            for _, sourceTable := range sourceTableData {
                wg.Add(1)
                go diff(sourceDbConfig, targetDbConfig, sourceDb, targetDb, sourceSchema, sourceTable, targetTableMap)
            }

            wg.Wait()

            // Print Sql...
            if len(diffSqlKeys) > 0 && len(diffSqlMap) > 0 {
                fmt.Println(fmt.Sprintf("SET NAMES %s;\n", sourceSchema.DefaultCharacterSetName))
                fmt.Println("SET FOREIGN_KEY_CHECKS=0;")
                fmt.Println()

                sort.Strings(diffSqlKeys)

                for k, diffSqlKey := range diffSqlKeys {
                    if diffSql, ok := diffSqlMap[diffSqlKey]; ok {
                        if k < len(diffSqlKeys)-1 {
                            fmt.Println(diffSql)
                            fmt.Println()
                        } else {
                            fmt.Println(diffSql)
                        }
                    }
                }

                fmt.Println()
                fmt.Println("SET FOREIGN_KEY_CHECKS=1;")
            }
        },
    }
)

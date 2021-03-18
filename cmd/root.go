package cmd

import (
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sort"
	"strconv"
	"strings"
)

type DbConfig struct {
	User     string
	Password string
	Host     string
	Port     int
	Database string
	Charset  string
}

type Schema struct {
	CatalogName             string         `gorm:"column:CATALOG_NAME"`
	SchemaName              string         `gorm:"column:SCHEMA_NAME"`
	DefaultCharacterSetName string         `gorm:"column:DEFAULT_CHARACTER_SET_NAME"`
	DefaultCollationName    string         `gorm:"column:DEFAULT_COLLATION_NAME"`
	SqlPath                 sql.NullString `gorm:"column:SQL_PATH"`
}

type Table struct {
	TableCatalog   string         `gorm:"column:TABLE_CATALOG"`
	TableSchema    string         `gorm:"column:TABLE_SCHEMA"`
	TableName      string         `gorm:"column:TABLE_NAME"`
	TableType      string         `gorm:"column:TABLE_TYPE"`
	ENGINE         sql.NullString `gorm:"column:ENGINE"`
	VERSION        sql.NullInt64  `gorm:"column:VERSION"`
	RowFormat      sql.NullString `gorm:"column:ROW_FORMAT"`
	TableRows      sql.NullInt64  `gorm:"column:TABLE_ROWS"`
	AvgRowLength   sql.NullInt64  `gorm:"column:AVG_ROW_LENGTH"`
	DataLength     sql.NullInt64  `gorm:"column:DATA_LENGTH"`
	MaxDataLength  sql.NullInt64  `gorm:"column:MAX_DATA_LENGTH"`
	IndexLength    sql.NullInt64  `gorm:"column:INDEX_LENGTH"`
	DataFree       sql.NullInt64  `gorm:"column:DATA_FREE"`
	AutoIncrement  sql.NullInt64  `gorm:"column:AUTO_INCREMENT"`
	CreateTime     sql.NullTime   `gorm:"column:CREATE_TIME"`
	UpdateTime     sql.NullTime   `gorm:"column:UPDATE_TIME"`
	CheckTime      sql.NullTime   `gorm:"column:CHECK_TIME"`
	TableCollation sql.NullString `gorm:"column:TABLE_COLLATION"`
	CHECKSUM       sql.NullInt64  `gorm:"column:CHECKSUM"`
	CreateOptions  sql.NullString `gorm:"column:CREATE_OPTIONS"`
	TableComment   string         `gorm:"column:TABLE_COMMENT"`
}

type Column struct {
	TableCatalog           string         `gorm:"column:TABLE_CATALOG"`
	TableSchema            string         `gorm:"column:TABLE_SCHEMA"`
	TableName              string         `gorm:"column:TABLE_NAME"`
	ColumnName             string         `gorm:"column:COLUMN_NAME"`
	OrdinalPosition        int            `gorm:"column:ORDINAL_POSITION"`
	ColumnDefault          sql.NullString `gorm:"column:COLUMN_DEFAULT"`
	IsNullable             string         `gorm:"column:IS_NULLABLE"`
	DataType               string         `gorm:"column:DATA_TYPE"`
	CharacterMaximumLength sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
	CharacterOctetLength   sql.NullInt64  `gorm:"column:CHARACTER_OCTET_LENGTH"`
	NumericPrecision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
	NumericScale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
	DatetimePrecision      sql.NullInt64  `gorm:"column:DATETIME_PRECISION"`
	CharacterSetName       sql.NullString `gorm:"column:CHARACTER_SET_NAME"`
	CollationName          sql.NullString `gorm:"column:COLLATION_NAME"`
	ColumnType             string         `gorm:"column:COLUMN_TYPE"`
	ColumnKey              string         `gorm:"column:COLUMN_KEY"`
	EXTRA                  string         `gorm:"column:EXTRA"`
	PRIVILEGES             string         `gorm:"column:PRIVILEGES"`
	ColumnComment          string         `gorm:"column:COLUMN_COMMENT"`
	GenerationExpression   string         `gorm:"column:GENERATION_EXPRESSION"`
}

type Statistic struct {
	TableCatalog string         `gorm:"column:TABLE_CATALOG"`
	TableSchema  string         `gorm:"column:TABLE_SCHEMA"`
	TableName    string         `gorm:"column:TABLE_NAME"`
	NonUnique    int64          `gorm:"column:NON_UNIQUE"`
	IndexSchema  string         `gorm:"column:INDEX_SCHEMA"`
	IndexName    string         `gorm:"column:INDEX_NAME"`
	SeqInIndex   int            `gorm:"column:SEQ_IN_INDEX"`
	ColumnName   string         `gorm:"column:COLUMN_NAME"`
	COLLATION    sql.NullString `gorm:"column:COLLATION"`
	CARDINALITY  sql.NullInt64  `gorm:"column:CARDINALITY"`
	SubPart      sql.NullInt32  `gorm:"column:SUB_PART"`
	PACKED       sql.NullString `gorm:"column:PACKED"`
	NULLABLE     string         `gorm:"column:NULLABLE"`
	IndexType    string         `gorm:"column:INDEX_TYPE"`
	COMMENT      sql.NullString `gorm:"column:COMMENT"`
	IndexComment string         `gorm:"column:INDEX_COMMENT"`
	IsVisible    sql.NullString `gorm:"column:IS_VISIBLE"`
}

var (
	source string
	target string
	db     string

	rootCmd = &cobra.Command{
		Use:   "mysqldiff",
		Short: "差异 SQL 工具。",
		Run: func(cmd *cobra.Command, args []string) {
			var (
				dns        = "%s:%s@tcp(%s:%d)/information_schema?parseTime=true&charset=%s"
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
				DSN: fmt.Sprintf(dns,
					sourceDbConfig.User, sourceDbConfig.Password,
					sourceDbConfig.Host, sourceDbConfig.Port,
					sourceDbConfig.Charset,
				),
			}), &gorm.Config{SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Error)})

			cobra.CheckErr(err)

			var targetDb = sourceDb

			if target != "" {
				var targetUser = strings.Split(target[0:strings.LastIndex(target, "@")], ":")
				var targetHost = strings.Split(target[strings.LastIndex(target, "@")+1:], ":")

				targetDbConfig.User = targetUser[0]
				targetDbConfig.Password = targetUser[1]
				targetDbConfig.Host = targetHost[0]
				targetDbConfig.Port, err = strconv.Atoi(targetHost[1])
				cobra.CheckErr(err)

				targetDb, err = gorm.Open(mysql.New(mysql.Config{
					DSN: fmt.Sprintf(dns,
						targetDbConfig.User, targetDbConfig.Password,
						targetDbConfig.Host, targetDbConfig.Port,
						targetDbConfig.Charset,
					),
				}), &gorm.Config{SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Error)})

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

			var diffSql []string

			// DROP TABLE...
			for _, targetTable := range targetTableData {
				if _, ok := sourceTableMap[targetTable.TableName]; !ok {
					diffSql = append(diffSql, fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", targetTable.TableName))
				}
			}

			for _, sourceTable := range sourceTableData {
				if _, ok := targetTableMap[sourceTable.TableName]; ok {
					var (
						sourceColumnData []Column
						targetColumnData []Column
					)

					// ALTER TABLE ...
					sourceDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
						&sourceColumnData,
						"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
						sourceDbConfig.Database, sourceTable.TableName,
					)
					targetDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
						&targetColumnData,
						"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
						targetDbConfig.Database, sourceTable.TableName,
					)

					sourceColumnDataLen := len(sourceColumnData)
					targetColumnDataLen := len(targetColumnData)

					// ALTER LIST ...
					var (
						alterTableSql  []string
						alterColumnSql []string
						alterKeySql    []string
					)

					if sourceColumnDataLen > 0 && targetColumnDataLen > 0 {
						sourceColumns := make(map[string]Column)
						targetColumns := make(map[string]Column)
						sourceColumnsPos := make(map[int]Column)
						targetColumnsPos := make(map[int]Column)

						for _, sourceColumn := range sourceColumnData {
							sourceColumns[sourceColumn.ColumnName] = sourceColumn
							sourceColumnsPos[sourceColumn.OrdinalPosition] = sourceColumn
						}

						for _, targetColumn := range targetColumnData {
							targetColumns[targetColumn.ColumnName] = targetColumn
							targetColumnsPos[targetColumn.OrdinalPosition] = targetColumn
						}

						if !CompareColumns(sourceColumnsPos, targetColumnsPos) {
							alterTableSql = append(alterTableSql, fmt.Sprintf("ALTER TABLE `%s`", sourceTable.TableName))

							// DROP COLUMN ...
							for _, targetColumn := range targetColumns {
								if _, ok := sourceColumns[targetColumn.ColumnName]; !ok {
									ResetCalcPosition(targetColumn.ColumnName, targetColumn.OrdinalPosition, targetColumns, 3)

									alterColumnSql = append(alterColumnSql, fmt.Sprintf("  DROP COLUMN `%s`",
										targetColumn.ColumnName,
									))
								}
							}

							// ADD COLUMN ...
							for _, sourceColumn := range sourceColumnData {
								if _, ok := targetColumns[sourceColumn.ColumnName]; !ok {
									alterColumnSql = append(alterColumnSql, fmt.Sprintf(
										"  ADD COLUMN `%s` %s%s%s%s %s",
										sourceColumn.ColumnName, sourceColumn.ColumnType,
										GetCharacterSet(sourceColumn, sourceSchema),
										GetColumnNullAbleDefault(sourceColumn),
										GetColumnExtra(sourceColumn),
										GetColumnAfter(sourceColumn.OrdinalPosition, sourceColumnsPos),
									))

									ResetCalcPosition(sourceColumn.ColumnName, sourceColumn.OrdinalPosition, targetColumns, 1)
								}
							}

							// MODIFY COLUMN ...
							for _, sourceColumn := range sourceColumnData {
								columnName := sourceColumn.ColumnName

								if _, ok := targetColumns[columnName]; ok {
									if !CompareColumn(sourceColumn, targetColumns[columnName]) {
										alterColumnSql = append(alterColumnSql,
											fmt.Sprintf("  MODIFY COLUMN `%s` %s%s%s%s %s",
												columnName, sourceColumn.ColumnType,
												GetCharacterSet(sourceColumn, sourceSchema),
												GetColumnNullAbleDefault(sourceColumn),
												GetColumnExtra(sourceColumn),
												GetColumnAfter(sourceColumn.OrdinalPosition, sourceColumnsPos),
											),
										)

										ResetCalcPosition(columnName, sourceColumn.OrdinalPosition, targetColumns, 2)
									}
								}
							}
						}
					}

					// ADD KEY AND DROP INDEX ...
					var (
						sourceStatisticsData []Statistic
						targetStatisticsData []Statistic
					)

					sourceDb.Table("STATISTICS").Find(
						&sourceStatisticsData,
						"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
						sourceDbConfig.Database, sourceTable.TableName,
					)

					targetDb.Table("STATISTICS").Find(
						&targetStatisticsData,
						"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
						targetDbConfig.Database, sourceTable.TableName,
					)

					sourceStatisticsDataLen := len(sourceStatisticsData)

					if sourceStatisticsDataLen > 0 {
						sourceStatisticsDataMap := make(map[string]map[int]Statistic)
						targetStatisticsDataMap := make(map[string]map[int]Statistic)

						for _, sourceStatistic := range sourceStatisticsData {
							if _, ok := sourceStatisticsDataMap[sourceStatistic.IndexName]; ok {
								sourceStatisticsDataMap[sourceStatistic.IndexName][sourceStatistic.SeqInIndex] = sourceStatistic
							} else {
								sourceSeqInIndexStatisticMap := make(map[int]Statistic)
								sourceSeqInIndexStatisticMap[sourceStatistic.SeqInIndex] = sourceStatistic
								sourceStatisticsDataMap[sourceStatistic.IndexName] = sourceSeqInIndexStatisticMap
							}
						}

						for _, targetStatistic := range targetStatisticsData {
							if _, ok := targetStatisticsDataMap[targetStatistic.IndexName]; ok {
								targetStatisticsDataMap[targetStatistic.IndexName][targetStatistic.SeqInIndex] = targetStatistic
							} else {
								targetSeqInIndexStatisticMap := make(map[int]Statistic)
								targetSeqInIndexStatisticMap[targetStatistic.SeqInIndex] = targetStatistic
								targetStatisticsDataMap[targetStatistic.IndexName] = targetSeqInIndexStatisticMap
							}
						}

						if !CompareStatistics(sourceStatisticsDataMap, targetStatisticsDataMap) {
							if len(alterTableSql) <= 0 {
								alterTableSql = append(alterTableSql, fmt.Sprintf("ALTER TABLE `%s`", sourceTable.TableName))
							}

							// DROP INDEX ...
							for targetIndexName, _ := range targetStatisticsDataMap {
								if _, ok := sourceStatisticsDataMap[targetIndexName]; !ok {
									if "PRIMARY" == targetIndexName {
										alterKeySql = append(alterKeySql, "  DROP PRIMARY KEY")
									} else {
										alterKeySql = append(alterKeySql, fmt.Sprintf("  DROP INDEX `%s`", targetIndexName))
									}
								}
							}

							// DROP INDEX ... AND ADD KEY ...
							for sourceIndexName, sourceStatisticMap := range sourceStatisticsDataMap {
								if _, ok := targetStatisticsDataMap[sourceIndexName]; ok {
									if !CompareStatisticsIndex(sourceStatisticMap, targetStatisticsDataMap[sourceIndexName]) {
										// DROP INDEX ...
										if "PRIMARY" == sourceIndexName {
											alterKeySql = append(alterKeySql, "  DROP PRIMARY KEY")
										} else {
											alterKeySql = append(alterKeySql, fmt.Sprintf("  DROP INDEX `%s`", sourceIndexName))
										}

										// ADD KEY ...
										alterKeySql = append(alterKeySql, fmt.Sprintf("  ADD %s", GetAddKeys(sourceIndexName, sourceStatisticMap)))
									}
								} else {
									// ADD KEY ...
									alterKeySql = append(alterKeySql, fmt.Sprintf("  ADD %s", GetAddKeys(sourceIndexName, sourceStatisticMap)))
								}
							}

							if len(alterKeySql) > 0 {
								for _, keySql := range alterKeySql {
									alterColumnSql = append(alterColumnSql, keySql)
								}
							}
						}
					}

					// ALTER TABLE SQL ...
					alterColumnSqlLen := len(alterColumnSql)

					if alterColumnSqlLen > 0 {
						for _, alterColumn := range alterColumnSql {
							var columnDot = ""
							if alterColumn == alterColumnSql[alterColumnSqlLen-1] {
								columnDot = ";"
							} else {
								columnDot = ","
							}

							alterTableSql = append(alterTableSql, fmt.Sprintf("%s%s", alterColumn, columnDot))
						}
					}

					alterTableSqlLen := len(alterTableSql)

					if alterTableSqlLen > 0 {
						diffSql = append(diffSql, strings.Join(alterTableSql, "\n"))
					}
				} else {
					// CREATE TABLE ...
					var (
						sourceColumnData []Column
					)

					sourceDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
						&sourceColumnData,
						"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
						sourceDbConfig.Database, sourceTable.TableName,
					)

					sourceColumnDataLen := len(sourceColumnData)

					if sourceColumnDataLen > 0 {
						var sourceStatisticsData []Statistic

						sourceDb.Table("STATISTICS").Find(
							&sourceStatisticsData,
							"`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
							sourceDbConfig.Database, sourceTable.TableName,
						)

						var createTableSql []string

						createTableSql = append(createTableSql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", sourceTable.TableName))

						// COLUMNS ...
						for _, sourceColumn := range sourceColumnData {
							var (
								dot = ""
							)

							if sourceColumn != sourceColumnData[sourceColumnDataLen-1] || len(sourceStatisticsData) > 0 {
								dot = ","
							}

							createTableSql = append(createTableSql, fmt.Sprintf("  `%s` %s%s%s%s%s",
								sourceColumn.ColumnName, sourceColumn.ColumnType,
								GetCharacterSet(sourceColumn, sourceSchema),
								GetColumnNullAbleDefault(sourceColumn),
								GetColumnExtra(sourceColumn), dot,
							))
						}

						// KEY ...
						var createKeySql []string
						sourceStatisticsLen := len(sourceStatisticsData)

						if sourceStatisticsLen > 0 {
							var sourceStatisticIndexNameArray []string
							sourceStatisticsDataMap := make(map[string]map[int]Statistic)

							for _, sourceStatistic := range sourceStatisticsData {
								if _, ok := sourceStatisticsDataMap[sourceStatistic.IndexName]; ok {
									sourceStatisticsDataMap[sourceStatistic.IndexName][sourceStatistic.SeqInIndex] = sourceStatistic
								} else {
									sourceSeqInIndexStatisticMap := make(map[int]Statistic)
									sourceSeqInIndexStatisticMap[sourceStatistic.SeqInIndex] = sourceStatistic
									sourceStatisticsDataMap[sourceStatistic.IndexName] = sourceSeqInIndexStatisticMap
								}

								if !inArray(sourceStatistic.IndexName, sourceStatisticIndexNameArray) {
									sourceStatisticIndexNameArray = append(sourceStatisticIndexNameArray, sourceStatistic.IndexName)
								}
							}

							for _, sourceIndexName := range sourceStatisticIndexNameArray {
								createKeySql = append(createKeySql, fmt.Sprintf("  %s", GetAddKeys(sourceIndexName, sourceStatisticsDataMap[sourceIndexName])))
							}
						}

						createTableSql = append(createTableSql, strings.Join(createKeySql, ",\n"))
						createTableSql = append(createTableSql, fmt.Sprintf(") ENGINE=%s DEFAULT CHARSET=%s;", sourceTable.ENGINE.String, sourceSchema.DefaultCharacterSetName))

						diffSql = append(diffSql, strings.Join(createTableSql, "\n"))
					}
				}
			}

			// Print Sql...
			if len(diffSql) > 0 {
				cmd.Println(fmt.Sprintf("SET NAMES %s;\n", sourceSchema.DefaultCharacterSetName))
				cmd.Println(strings.Join(diffSql, "\n\n"))
			}
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.Flags().StringVarP(&source, "source", "", "", "指定源服务器。(格式: <user>:<password>@<host>:<port>)")
	rootCmd.Flags().StringVarP(&target, "target", "", "", "指定目标服务器。(格式: <user>:<password>@<host>:<port>)")
	rootCmd.Flags().StringVarP(&db, "db", "", "", "指定数据库。(格式: <source_db>:<target_db>)")

	cobra.CheckErr(rootCmd.MarkFlagRequired("source"))
	cobra.CheckErr(rootCmd.MarkFlagRequired("db"))

	rootCmd.AddCommand(completionCmd)
}

func initConfig() {
}

func GetColumnNullAbleDefault(column Column) string {
	var nullAbleDefault = ""

	if column.IsNullable == "NO" {
		if column.ColumnDefault.Valid {
			if inArray(column.DataType, []string{"timestamp", "datetime"}) {
				nullAbleDefault = fmt.Sprintf(" NOT NULL DEFAULT %s", column.ColumnDefault.String)
			} else {
				nullAbleDefault = fmt.Sprintf(" NOT NULL DEFAULT '%s'", column.ColumnDefault.String)
			}
		} else {
			nullAbleDefault = " NOT NULL"
		}
	} else {
		if column.ColumnDefault.Valid {
			if inArray(column.DataType, []string{"timestamp", "datetime"}) {
				nullAbleDefault = fmt.Sprintf(" NULL DEFAULT %s", column.ColumnDefault.String)
			} else {
				nullAbleDefault = fmt.Sprintf(" DEFAULT '%s'", column.ColumnDefault.String)
			}
		} else {
			nullAbleDefault = " DEFAULT NULL"
		}
	}

	return nullAbleDefault
}

func GetAddKeys(indexName string, statisticMap map[int]Statistic) string {
	if 1 == statisticMap[1].NonUnique {
		var seqInIndexSort []int
		var columnNames []string

		for seqInIndex, _ := range statisticMap {
			seqInIndexSort = append(seqInIndexSort, seqInIndex)
		}

		sort.Ints(seqInIndexSort)

		for _, seqInIndex := range seqInIndexSort {
			var subPart = ""

			if statisticMap[seqInIndex].SubPart.Valid {
				subPart = fmt.Sprintf("(%d)", statisticMap[seqInIndex].SubPart.Int32)
			}

			columnNames = append(columnNames, fmt.Sprintf("`%s`%s", statisticMap[seqInIndex].ColumnName, subPart))
		}

		return fmt.Sprintf("KEY `%s` (%s)", indexName, strings.Join(columnNames, ","))
	} else {
		if "PRIMARY" == indexName {
			var seqInIndexSort []int
			var columnNames []string

			for seqInIndex, _ := range statisticMap {
				seqInIndexSort = append(seqInIndexSort, seqInIndex)
			}

			sort.Ints(seqInIndexSort)

			for _, seqInIndex := range seqInIndexSort {
				var subPart = ""

				if statisticMap[seqInIndex].SubPart.Valid {
					subPart = fmt.Sprintf("(%d)", statisticMap[seqInIndex].SubPart.Int32)
				}

				columnNames = append(columnNames, fmt.Sprintf("`%s`%s", statisticMap[seqInIndex].ColumnName, subPart))
			}

			return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(columnNames, ","))
		} else {
			var seqInIndexSort []int
			var columnNames []string

			for seqInIndex, _ := range statisticMap {
				seqInIndexSort = append(seqInIndexSort, seqInIndex)
			}

			sort.Ints(seqInIndexSort)

			for _, seqInIndex := range seqInIndexSort {
				var subPart = ""

				if statisticMap[seqInIndex].SubPart.Valid {
					subPart = fmt.Sprintf("(%d)", statisticMap[seqInIndex].SubPart.Int32)
				}

				columnNames = append(columnNames, fmt.Sprintf("`%s`%s", statisticMap[seqInIndex].ColumnName, subPart))
			}

			return fmt.Sprintf("UNIQUE KEY `%s` (%s)", indexName, strings.Join(columnNames, ","))
		}
	}
}

func CompareColumns(sourceColumnsPos map[int]Column, targetColumnsPos map[int]Column) bool {
	if len(sourceColumnsPos) != len(targetColumnsPos) {
		return false
	} else {
		for sourcePos, sourceColumn := range sourceColumnsPos {
			if _, ok := targetColumnsPos[sourcePos]; ok {
				targetColumn := targetColumnsPos[sourcePos]

				if !CompareColumn(sourceColumn, targetColumn) {
					return false
				}
			} else {
				return false
			}

		}
	}

	return true
}

func CompareColumn(sourceColumn Column, targetColumn Column) bool {
	if sourceColumn.ColumnName != targetColumn.ColumnName {
		return false
	}

	if sourceColumn.OrdinalPosition != targetColumn.OrdinalPosition {
		return false
	}

	if sourceColumn.ColumnDefault != targetColumn.ColumnDefault {
		return false
	}

	if sourceColumn.IsNullable != targetColumn.IsNullable {
		return false
	}

	if sourceColumn.DataType != targetColumn.DataType {
		return false
	}

	if sourceColumn.CharacterMaximumLength != targetColumn.CharacterMaximumLength {
		return false
	}

	//禁用实际精度检验，因为 TiDB 和 MySQL 在设置不标准的情况下，值会不一样。
	//if sourceColumn.NumericPrecision != targetColumn.NumericPrecision {
	//	return false
	//}

	if sourceColumn.NumericScale != targetColumn.NumericScale {
		return false
	}

	if sourceColumn.DatetimePrecision != targetColumn.DatetimePrecision {
		return false
	}

	if sourceColumn.CharacterSetName != targetColumn.CharacterSetName {
		return false
	}

	if sourceColumn.CollationName != targetColumn.CollationName {
		return false
	}

	if sourceColumn.ColumnType != targetColumn.ColumnType {
		return false
	}

	if sourceColumn.EXTRA != targetColumn.EXTRA {
		return false
	}

	return true
}

func CompareStatistics(sourceStatisticsMap map[string]map[int]Statistic, targetStatisticsMap map[string]map[int]Statistic) bool {
	if len(sourceStatisticsMap) != len(targetStatisticsMap) {
		return false
	} else {
		for indexName, sourceStatisticMap := range sourceStatisticsMap {
			if _, ok := targetStatisticsMap[indexName]; ok {
				if len(sourceStatisticMap) != len(targetStatisticsMap[indexName]) {
					return false
				} else {
					for seqInIndex, sourceStatistic := range sourceStatisticMap {
						if _, ok := targetStatisticsMap[indexName][seqInIndex]; ok {
							if !CompareStatistic(sourceStatistic, targetStatisticsMap[indexName][seqInIndex]) {
								return false
							}
						} else {
							return false
						}
					}
				}
			} else {
				return false
			}
		}
	}

	return true
}

func CompareStatisticsIndex(sourceStatisticMap map[int]Statistic, targetStatisticMap map[int]Statistic) bool {
	if len(sourceStatisticMap) != len(targetStatisticMap) {
		return false
	} else {
		for seqInIndex, sourceStatistic := range sourceStatisticMap {
			if _, ok := targetStatisticMap[seqInIndex]; ok {
				if !CompareStatistic(sourceStatistic, targetStatisticMap[seqInIndex]) {
					return false
				}
			} else {
				return false
			}
		}
	}

	return true
}

func CompareStatistic(sourceStatistic Statistic, targetStatistic Statistic) bool {
	if sourceStatistic.NonUnique != targetStatistic.NonUnique {
		return false
	}

	if sourceStatistic.IndexName != targetStatistic.IndexName {
		return false
	}

	if sourceStatistic.SeqInIndex != targetStatistic.SeqInIndex {
		return false
	}

	if sourceStatistic.ColumnName != targetStatistic.ColumnName {
		return false
	}

	if sourceStatistic.SubPart != targetStatistic.SubPart {
		return false
	}

	if sourceStatistic.IndexType != targetStatistic.IndexType {
		return false
	}

	return true
}

func ResetCalcPosition(columnName string, sourcePos int, targetColumns map[string]Column, status int) {
	switch status {
	case 1:
		// ADD ...
		for targetColumnName, targetColumn := range targetColumns {
			if targetColumn.OrdinalPosition >= sourcePos {
				targetColumn.OrdinalPosition += 1

				targetColumns[targetColumnName] = targetColumn
			}
		}
		break
	case 2:
		// MODIFY ...
		if _, ok := targetColumns[columnName]; ok {
			targetColumn := targetColumns[columnName]

			targetColumn.OrdinalPosition = sourcePos

			targetColumns[columnName] = targetColumn
		}
		break
	case 3:
		// DROP ...
		for targetColumnName, targetColumn := range targetColumns {
			if targetColumn.OrdinalPosition >= sourcePos {
				targetColumn.OrdinalPosition -= 1

				targetColumns[targetColumnName] = targetColumn
			}
		}
		break
	}
}

func GetColumnAfter(ordinalPosition int, columnsPos map[int]Column) string {
	pos := ordinalPosition - 1

	if _, ok := columnsPos[pos]; ok {
		return fmt.Sprintf("AFTER `%s`", columnsPos[pos].ColumnName)
	} else {
		return "FIRST"
	}
}

func GetCharacterSet(column Column, schema Schema) string {
	if column.CharacterSetName.Valid {
		if column.CharacterSetName.String != schema.DefaultCharacterSetName {
			return fmt.Sprintf(" CHARACTER SET %s", column.CharacterSetName.String)
		}
	}

	return ""
}

func GetColumnExtra(column Column) string {
	extra := strings.TrimSpace(strings.Replace(strings.ToUpper(column.EXTRA), "DEFAULT_GENERATED", "", 1))

	if extra != "" {
		return fmt.Sprintf(" %s", extra)
	}

	return ""
}

func inArray(need string, needArr []string) bool {
	for _, v := range needArr {
		if need == v {
			return true
		}
	}
	return false
}

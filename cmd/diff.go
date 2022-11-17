package cmd

import (
    "fmt"
    "github.com/camry/g/gutil"
    "gorm.io/gorm"
    "strings"
)

// DROP TABLE Or DROP VIEW...
func drop(sourceTableMap map[string]Table, targetTableData []Table) {
    for _, targetTable := range targetTableData {
        if _, ok := sourceTableMap[targetTable.TableName]; !ok {
            switch targetTable.TableType {
            case "BASE TABLE":
                diffSqlKeys = append(diffSqlKeys, targetTable.TableName)
                diffSqlMap[targetTable.TableName] = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", targetTable.TableName)
            case "VIEW":
                diffSqlKeys = append(diffSqlKeys, targetTable.TableName)
                diffSqlMap[targetTable.TableName] = fmt.Sprintf("DROP VIEW IF EXISTS `%s`;", targetTable.TableName)
            }
        }
    }
}

// SQL DIFF ...
func diff(sourceDbConfig DbConfig, targetDbConfig DbConfig, sourceDb *gorm.DB, targetDb *gorm.DB, sourceSchema Schema, sourceTable Table, targetTableMap map[string]Table) {
    defer wg.Done()

    ch <- true

    switch sourceTable.TableType {
    case "BASE TABLE":
        if _, ok := targetTableMap[sourceTable.TableName]; ok {
            alterTable(sourceDbConfig, targetDbConfig, sourceDb, targetDb, sourceTable, targetTableMap)
        } else {
            createTable(sourceDbConfig, sourceDb, sourceSchema, sourceTable)
        }
    case "VIEW":
        createView(sourceDbConfig, targetDbConfig, sourceDb, targetDb, sourceTable, targetTableMap)
    }

    <-ch
}

// CREATE TABLE ...
func createTable(sourceDbConfig DbConfig, sourceDb *gorm.DB, sourceSchema Schema, sourceTable Table) {
    var (
        sourceColumnData     []Column
        sourceStatisticsData []Statistic
    )

    sourceDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
        &sourceColumnData,
        "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        sourceDbConfig.Database, sourceTable.TableName,
    )

    sourceColumnDataLen := len(sourceColumnData)

    if sourceColumnDataLen > 0 {
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

            createSql := fmt.Sprintf("  `%s` %s%s%s%s",
                sourceColumn.ColumnName, sourceColumn.ColumnType,
                getCharacterSet(sourceColumn, sourceColumn),
                getColumnNullAbleDefault(sourceColumn),
                getColumnExtra(sourceColumn),
            )

            if comment {
                createSql = fmt.Sprintf("%s COMMENT '%s'", createSql, getColumnComment(sourceColumn.ColumnComment))
            }

            createTableSql = append(createTableSql, fmt.Sprintf("%s%s", createSql, dot))
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

                if gutil.InArray(sourceStatistic.IndexName, sourceStatisticIndexNameArray) {
                    sourceStatisticIndexNameArray = append(sourceStatisticIndexNameArray, sourceStatistic.IndexName)
                }
            }

            for _, sourceIndexName := range sourceStatisticIndexNameArray {
                createKeySql = append(createKeySql, fmt.Sprintf("  %s", getAddKeys(sourceIndexName, sourceStatisticsDataMap[sourceIndexName])))
            }
        }

        if foreign {
            // CONSTRAINT [symbol] FOREIGN KEY (col_name, ...) REFERENCES tbl_name (col_name,...) [ON DELETE reference_option] [ON UPDATE reference_option]
            var sourceTableConstraints []TableConstraints

            tx1 := sourceDb.Table("TABLE_CONSTRAINTS").Find(&sourceTableConstraints,
                "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `CONSTRAINT_TYPE` = ?",
                sourceTable.TableSchema, sourceTable.TableName, "FOREIGN KEY",
            )

            if tx1.RowsAffected > 0 {
                for _, sourceTableConstraint := range sourceTableConstraints {
                    constraintSql := getConstraint(sourceDb, sourceTable, sourceTableConstraint)

                    if constraintSql != "" {
                        createKeySql = append(createKeySql, fmt.Sprintf("  %s", constraintSql))
                    }
                }
            }
        }

        createTableSql = append(createTableSql, strings.Join(createKeySql, ",\n"))

        cSql := ""

        if comment {
            cSql = fmt.Sprintf(" COMMENT='%s'", getColumnComment(sourceTable.TableComment))
        }

        charset := sourceSchema.DefaultCharacterSetName
        collate := sourceSchema.DefaultCollationName

        if sourceTable.TableCollation.Valid {
            charset = strings.Split(sourceTable.TableCollation.String, "_")[0]
            collate = sourceTable.TableCollation.String
        }

        createTableSql = append(createTableSql, fmt.Sprintf(") ENGINE=%s DEFAULT CHARSET=%s COLLATE=%s%s;",
            sourceTable.ENGINE.String, charset, collate, cSql,
        ))

        lock.Lock()

        diffSqlKeys = append(diffSqlKeys, sourceTable.TableName)
        diffSqlMap[sourceTable.TableName] = strings.Join(createTableSql, "\n")

        lock.Unlock()
    }
}

// ALTER TABLE ...
func alterTable(sourceDbConfig DbConfig, targetDbConfig DbConfig, sourceDb *gorm.DB, targetDb *gorm.DB, sourceTable Table, targetTableMap map[string]Table) {
    targetTable := targetTableMap[sourceTable.TableName]

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

        if !compareColumns(sourceColumnsPos, targetColumnsPos) {
            // DROP COLUMN ...
            for _, targetColumn := range targetColumns {
                if _, ok := sourceColumns[targetColumn.ColumnName]; !ok {
                    resetCalcPosition(targetColumn.ColumnName, targetColumn.OrdinalPosition, targetColumns, 3)

                    alterColumnSql = append(alterColumnSql, fmt.Sprintf("  DROP COLUMN `%s`",
                        targetColumn.ColumnName,
                    ))
                }
            }

            // ADD COLUMN ...
            for _, sourceColumn := range sourceColumnData {
                if _, ok := targetColumns[sourceColumn.ColumnName]; !ok {
                    addSql := fmt.Sprintf(
                        "  ADD COLUMN `%s` %s%s%s%s",
                        sourceColumn.ColumnName, sourceColumn.ColumnType,
                        getCharacterSet(sourceColumn, targetColumns[sourceColumn.ColumnName]),
                        getColumnNullAbleDefault(sourceColumn),
                        getColumnExtra(sourceColumn),
                    )

                    if comment {
                        addSql = fmt.Sprintf("%s COMMENT '%s'", addSql, getColumnComment(sourceColumn.ColumnComment))
                    }

                    alterColumnSql = append(alterColumnSql, fmt.Sprintf("%s %s",
                        addSql,
                        getColumnAfter(sourceColumn.OrdinalPosition, sourceColumnsPos),
                    ))

                    resetCalcPosition(sourceColumn.ColumnName, sourceColumn.OrdinalPosition, targetColumns, 1)
                }
            }

            // MODIFY COLUMN ...
            for _, sourceColumn := range sourceColumnData {
                columnName := sourceColumn.ColumnName

                if _, ok := targetColumns[columnName]; ok {
                    targetColumn := targetColumns[columnName]

                    if !compareColumn(sourceColumn, targetColumn) {
                        modifySql := fmt.Sprintf("  MODIFY COLUMN `%s` %s%s%s%s",
                            columnName, sourceColumn.ColumnType,
                            getCharacterSet(sourceColumn, targetColumn),
                            getColumnNullAbleDefault(sourceColumn),
                            getColumnExtra(sourceColumn),
                        )

                        if comment {
                            modifySql = fmt.Sprintf("%s COMMENT '%s'", modifySql, getColumnComment(sourceColumn.ColumnComment))
                        }

                        alterColumnSql = append(alterColumnSql, fmt.Sprintf("%s %s",
                            modifySql,
                            getColumnAfter(sourceColumn.OrdinalPosition, sourceColumnsPos),
                        ))

                        resetCalcPosition(columnName, sourceColumn.OrdinalPosition, targetColumns, 2)
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

        if !compareStatistics(sourceStatisticsDataMap, targetStatisticsDataMap) {
            // DROP INDEX ...
            for targetIndexName := range targetStatisticsDataMap {
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
                    if !compareStatisticsIndex(sourceStatisticMap, targetStatisticsDataMap[sourceIndexName]) {
                        // DROP INDEX ...
                        if "PRIMARY" == sourceIndexName {
                            alterKeySql = append(alterKeySql, "  DROP PRIMARY KEY")
                        } else {
                            alterKeySql = append(alterKeySql, fmt.Sprintf("  DROP INDEX `%s`", sourceIndexName))
                        }

                        // ADD KEY ...
                        alterKeySql = append(alterKeySql, fmt.Sprintf("  ADD %s", getAddKeys(sourceIndexName, sourceStatisticMap)))
                    }
                } else {
                    // ADD KEY ...
                    alterKeySql = append(alterKeySql, fmt.Sprintf("  ADD %s", getAddKeys(sourceIndexName, sourceStatisticMap)))
                }
            }

            if len(alterKeySql) > 0 {
                for _, keySql := range alterKeySql {
                    alterColumnSql = append(alterColumnSql, keySql)
                }
            }
        }
    }

    if foreign {
        // ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol;
        // CONSTRAINT [symbol] FOREIGN KEY (col_name, ...) REFERENCES tbl_name (col_name,...) [ON DELETE reference_option] [ON UPDATE reference_option]
        var (
            sourceTableConstraints []TableConstraints
            targetTableConstraints []TableConstraints
        )
        sourceTableConstraintsMap := make(map[string]TableConstraints)
        targetTableConstraintsMap := make(map[string]TableConstraints)

        tx1 := sourceDb.Table("TABLE_CONSTRAINTS").Find(&sourceTableConstraints,
            "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `CONSTRAINT_TYPE` = ?",
            sourceTable.TableSchema, sourceTable.TableName, "FOREIGN KEY",
        )

        tx2 := targetDb.Table("TABLE_CONSTRAINTS").Find(&targetTableConstraints,
            "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `CONSTRAINT_TYPE` = ?",
            targetTable.TableSchema, sourceTable.TableName, "FOREIGN KEY",
        )

        if tx1.RowsAffected > 0 {
            for _, sourceTableConstraint := range sourceTableConstraints {
                sourceTableConstraintsMap[sourceTableConstraint.ConstraintName] = sourceTableConstraint
            }
        }

        if tx2.RowsAffected > 0 {
            for _, targetTableConstraint := range targetTableConstraints {
                targetTableConstraintsMap[targetTableConstraint.ConstraintName] = targetTableConstraint

                if _, ok := sourceTableConstraintsMap[targetTableConstraint.ConstraintName]; !ok {
                    constraintSql := fmt.Sprintf("  DROP FOREIGN KEY `%s`", targetTableConstraint.ConstraintName)
                    alterColumnSql = append(alterColumnSql, constraintSql)
                }
            }
        }

        if tx1.RowsAffected > 0 {
            for _, sourceTableConstraint := range sourceTableConstraints {
                isAddConstraint := false

                if _, ok := targetTableConstraintsMap[sourceTableConstraint.ConstraintName]; ok {
                    if !compareConstraint(sourceDb, targetDb, sourceTable, targetTable, sourceTableConstraint, targetTableConstraintsMap[sourceTableConstraint.ConstraintName]) {
                        constraintSql := fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`;", sourceTable.TableName, sourceTableConstraint.ConstraintName)
                        alterTableSql = append(alterTableSql, constraintSql)
                        isAddConstraint = true
                    }
                } else {
                    isAddConstraint = true
                }

                if isAddConstraint {
                    constraintSql := getConstraint(sourceDb, sourceTable, sourceTableConstraint)
                    if constraintSql != "" {
                        alterColumnSql = append(alterColumnSql, fmt.Sprintf("  ADD %s", constraintSql))
                    }
                }
            }
        }
    }

    // ENGINE
    if sourceTable.ENGINE.Valid {
        if sourceTable.ENGINE.String != targetTable.ENGINE.String {
            alterColumnSql = append(alterColumnSql, fmt.Sprintf("  ENGINE=%s", sourceTable.ENGINE.String))
        }
    }

    // CHARACTER SET,COLLATE
    if sourceTable.TableCollation.Valid {
        if sourceTable.TableCollation.String != targetTable.TableCollation.String {
            charset := strings.Split(sourceTable.TableCollation.String, "_")[0]
            collate := sourceTable.TableCollation.String

            alterColumnSql = append(alterColumnSql, fmt.Sprintf("  CHARACTER SET=%s, COLLATE=%s",
                charset, collate,
            ))
        }
    }

    // COMMENT
    if comment {
        if sourceTable.TableComment != targetTable.TableComment {
            alterColumnSql = append(alterColumnSql, fmt.Sprintf("  COMMENT='%s'", sourceTable.TableComment))
        }
    }

    // ALTER TABLE SQL ...
    if tidb {
        if len(alterColumnSql) > 0 {
            for _, alterColumn := range alterColumnSql {
                alterTableSql = append(alterTableSql, fmt.Sprintf("ALTER TABLE `%s`", sourceTable.TableName))
                alterTableSql = append(alterTableSql, fmt.Sprintf("%s;", alterColumn))
            }
        }
    } else {
        alterColumnSqlLen := len(alterColumnSql)

        if alterColumnSqlLen > 0 {
            alterTableSql = append(alterTableSql, fmt.Sprintf("ALTER TABLE `%s`", sourceTable.TableName))

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
    }

    alterTableSqlLen := len(alterTableSql)

    if alterTableSqlLen > 0 {
        lock.Lock()

        diffSqlKeys = append(diffSqlKeys, sourceTable.TableName)
        diffSqlMap[sourceTable.TableName] = strings.Join(alterTableSql, "\n")

        lock.Unlock()
    }
}

// CREATE OR REPLACE VIEW ...
func createView(sourceDbConfig DbConfig, targetDbConfig DbConfig, sourceDb *gorm.DB, targetDb *gorm.DB, sourceTable Table, targetTableMap map[string]Table) {
    var (
        sourceView View
        targetView View
    )

    sourceDb.Table("VIEWS").First(
        &sourceView,
        "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        sourceDbConfig.Database, sourceTable.TableName,
    )

    sourceView.ViewDefinition = strings.Replace(sourceView.ViewDefinition, fmt.Sprintf("`%s`.", sourceDbConfig.Database), "", -1)

    if _, ok := targetTableMap[sourceTable.TableName]; ok {
        // CREATE OR REPLACE ...
        targetDb.Table("VIEWS").First(
            &targetView,
            "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
            targetDbConfig.Database, sourceTable.TableName,
        )

        targetView.ViewDefinition = strings.Replace(targetView.ViewDefinition, fmt.Sprintf("`%s`.", targetDbConfig.Database), "", -1)

        if sourceView.ViewDefinition != targetView.ViewDefinition {
            lock.Lock()

            diffSqlKeys = append(diffSqlKeys, sourceTable.TableName)
            diffSqlMap[sourceTable.TableName] = fmt.Sprintf("CREATE OR REPLACE ALGORITHM = UNDEFINED SQL SECURITY %s VIEW `%s` AS %s;",
                sourceView.SecurityType,
                sourceView.TableName,
                sourceView.ViewDefinition,
            )

            lock.Unlock()
        }
    } else {
        lock.Lock()

        // CREATE ...
        diffSqlKeys = append(diffSqlKeys, sourceTable.TableName)
        diffSqlMap[sourceTable.TableName] = fmt.Sprintf("CREATE ALGORITHM = UNDEFINED SQL SECURITY %s VIEW `%s` AS %s;",
            sourceView.SecurityType,
            sourceView.TableName,
            sourceView.ViewDefinition,
        )

        lock.Unlock()
    }
}

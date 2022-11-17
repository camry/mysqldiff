package cmd

import (
    "fmt"
    "strings"

    "gorm.io/gorm"
)

func resetCalcPosition(columnName string, sourcePos int, targetColumns map[string]Column, status int) {
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

func compareColumns(sourceColumnsPos map[int]Column, targetColumnsPos map[int]Column) bool {
    if len(sourceColumnsPos) != len(targetColumnsPos) {
        return false
    } else {
        for sourcePos, sourceColumn := range sourceColumnsPos {
            if _, ok := targetColumnsPos[sourcePos]; ok {
                targetColumn := targetColumnsPos[sourcePos]

                if !compareColumn(sourceColumn, targetColumn) {
                    return false
                }
            } else {
                return false
            }

        }
    }

    return true
}

func compareColumn(sourceColumn Column, targetColumn Column) bool {
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

    // 禁用实际精度检验，因为 TiDB 和 MySQL 在设置不标准的情况下，值会不一样。
    // if sourceColumn.NumericPrecision != targetColumn.NumericPrecision {
    //	return false
    // }

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

    if comment {
        if sourceColumn.ColumnComment != targetColumn.ColumnComment {
            return false
        }
    }

    return true
}

func compareStatistics(sourceStatisticsMap map[string]map[int]Statistic, targetStatisticsMap map[string]map[int]Statistic) bool {
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
                            targetStatistic := targetStatisticsMap[indexName][seqInIndex]

                            if !compareStatistic(sourceStatistic, targetStatistic) {
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

func compareStatisticsIndex(sourceStatisticMap map[int]Statistic, targetStatisticMap map[int]Statistic) bool {
    if len(sourceStatisticMap) != len(targetStatisticMap) {
        return false
    } else {
        for seqInIndex, sourceStatistic := range sourceStatisticMap {
            if _, ok := targetStatisticMap[seqInIndex]; ok {
                targetStatistic := targetStatisticMap[seqInIndex]

                if !compareStatistic(sourceStatistic, targetStatistic) {
                    return false
                }
            } else {
                return false
            }
        }
    }

    return true
}

func compareStatistic(sourceStatistic Statistic, targetStatistic Statistic) bool {
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

func compareConstraint(sourceDb *gorm.DB, targetDb *gorm.DB, sourceTable Table, targetTable Table, sourceTableConstraint TableConstraints, targetTableConstraint TableConstraints) bool {
    if sourceTableConstraint.ConstraintName != targetTableConstraint.ConstraintName {
        return false
    }

    if sourceTableConstraint.TableName != targetTableConstraint.TableName {
        return false
    }

    if sourceTableConstraint.ConstraintType != targetTableConstraint.ConstraintType {
        return false
    }

    var (
        sourceReferentialConstraint ReferentialConstraints
        targetReferentialConstraint ReferentialConstraints
    )

    tx1 := sourceDb.Table("REFERENTIAL_CONSTRAINTS").First(&sourceReferentialConstraint,
        "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ?",
        sourceTable.TableSchema, sourceTableConstraint.ConstraintName,
    )

    tx2 := targetDb.Table("REFERENTIAL_CONSTRAINTS").First(&targetReferentialConstraint,
        "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ?",
        targetTable.TableSchema, sourceTableConstraint.ConstraintName,
    )

    if tx1.RowsAffected <= 0 || tx2.RowsAffected <= 0 {
        return false
    }

    if sourceReferentialConstraint.UniqueConstraintName != targetReferentialConstraint.UniqueConstraintName {
        return false
    }

    if sourceReferentialConstraint.MatchOption != targetReferentialConstraint.MatchOption {
        return false
    }

    if sourceReferentialConstraint.UpdateRule != targetReferentialConstraint.UpdateRule {
        return false
    }

    if sourceReferentialConstraint.DeleteRule != targetReferentialConstraint.DeleteRule {
        return false
    }

    if sourceReferentialConstraint.TableName != targetReferentialConstraint.TableName {
        return false
    }

    if sourceReferentialConstraint.ReferencedTableName != targetReferentialConstraint.ReferencedTableName {
        return false
    }

    var (
        sourceKeyColumnUsages []KeyColumnUsage
        targetKeyColumnUsages []KeyColumnUsage
    )

    tx3 := sourceDb.Table("KEY_COLUMN_USAGE").Order("`POSITION_IN_UNIQUE_CONSTRAINT` ASC").Find(
        &sourceKeyColumnUsages,
        "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ? AND `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        sourceTable.TableSchema, sourceTableConstraint.ConstraintName, sourceTable.TableSchema, sourceTable.TableName,
    )

    tx4 := sourceDb.Table("KEY_COLUMN_USAGE").Order("`POSITION_IN_UNIQUE_CONSTRAINT` ASC").Find(
        &targetKeyColumnUsages,
        "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ? AND `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        targetTable.TableSchema, targetTableConstraint.ConstraintName, targetTable.TableSchema, targetTable.TableName,
    )

    if tx3.RowsAffected <= 0 || tx4.RowsAffected <= 0 {
        return false
    }

    sourceTableKeyColumns := make(map[string][]string)
    targetTableKeyColumns := make(map[string][]string)

    for _, sourceKeyColumnUsage := range sourceKeyColumnUsages {
        sourceTableKeyColumns[sourceReferentialConstraint.TableName] = append(sourceTableKeyColumns[sourceReferentialConstraint.TableName], fmt.Sprintf("`%s`", sourceKeyColumnUsage.ColumnName))
        sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName] = append(sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName], fmt.Sprintf("`%s`", sourceKeyColumnUsage.ReferencedColumnName))
    }

    for _, targetKeyColumnUsage := range targetKeyColumnUsages {
        targetTableKeyColumns[targetReferentialConstraint.TableName] = append(targetTableKeyColumns[targetReferentialConstraint.TableName], fmt.Sprintf("`%s`", targetKeyColumnUsage.ColumnName))
        targetTableKeyColumns[targetReferentialConstraint.ReferencedTableName] = append(targetTableKeyColumns[targetReferentialConstraint.ReferencedTableName], fmt.Sprintf("`%s`", targetKeyColumnUsage.ReferencedColumnName))
    }

    s1 := strings.Join(sourceTableKeyColumns[sourceReferentialConstraint.TableName], ",")
    t1 := strings.Join(targetTableKeyColumns[targetReferentialConstraint.TableName], ",")
    s2 := strings.Join(sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName], ",")
    t2 := strings.Join(targetTableKeyColumns[targetReferentialConstraint.ReferencedTableName], ",")

    if s1 != t1 || s2 != t2 {
        return false
    }

    return true
}

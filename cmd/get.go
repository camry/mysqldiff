package cmd

import (
    "fmt"
    "sort"
    "strings"

    "github.com/camry/g/gutil"
    "gorm.io/gorm"
)

func getColumnNullAbleDefault(column Column) string {
    var nullAbleDefault = ""

    if column.IsNullable == "NO" {
        if column.ColumnDefault.Valid {
            if gutil.InArray(column.DataType, []string{"timestamp", "datetime"}) {
                if column.ColumnDefault.String != "CURRENT_TIMESTAMP" {
                    column.ColumnDefault.String = fmt.Sprintf("'%s'", column.ColumnDefault.String)
                }

                nullAbleDefault = fmt.Sprintf(" NOT NULL DEFAULT %s", column.ColumnDefault.String)
            } else {
                nullAbleDefault = fmt.Sprintf(" NOT NULL DEFAULT '%s'", column.ColumnDefault.String)
            }
        } else {
            nullAbleDefault = " NOT NULL"
        }
    } else {
        if column.ColumnDefault.Valid {
            if gutil.InArray(column.DataType, []string{"timestamp", "datetime"}) {
                if column.ColumnDefault.String != "CURRENT_TIMESTAMP" {
                    column.ColumnDefault.String = fmt.Sprintf("'%s'", column.ColumnDefault.String)
                }

                nullAbleDefault = fmt.Sprintf(" NULL DEFAULT %s", column.ColumnDefault.String)
            } else {
                nullAbleDefault = fmt.Sprintf(" DEFAULT '%s'", column.ColumnDefault.String)
            }
        } else {
            if gutil.InArray(column.DataType, []string{"timestamp", "datetime"}) {
                nullAbleDefault = " NULL DEFAULT NULL"
            } else {
                nullAbleDefault = " DEFAULT NULL"
            }
        }
    }

    return nullAbleDefault
}

func getColumnAfter(ordinalPosition int, columnsPos map[int]Column) string {
    pos := ordinalPosition - 1

    if _, ok := columnsPos[pos]; ok {
        return fmt.Sprintf("AFTER `%s`", columnsPos[pos].ColumnName)
    } else {
        return "FIRST"
    }
}

func getColumnExtra(column Column) string {
    extra := strings.TrimSpace(strings.Replace(strings.ToUpper(column.EXTRA), "DEFAULT_GENERATED", "", 1))

    if extra != "" {
        return fmt.Sprintf(" %s", extra)
    }

    return ""
}

func getColumnComment(columnComment string) string {
    return strings.ReplaceAll(columnComment, "'", "\\'")
}

func getAddKeys(indexName string, statisticMap map[int]Statistic) string {
    if 1 == statisticMap[1].NonUnique {
        var seqInIndexSort []int
        var columnNames []string

        for seqInIndex := range statisticMap {
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

            for seqInIndex := range statisticMap {
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

            for seqInIndex := range statisticMap {
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

func getConstraint(sourceDb *gorm.DB, sourceTable Table, sourceTableConstraint TableConstraints) string {
    var sourceReferentialConstraint ReferentialConstraints

    tx1 := sourceDb.Table("REFERENTIAL_CONSTRAINTS").First(&sourceReferentialConstraint,
        "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ?",
        sourceTable.TableSchema, sourceTableConstraint.ConstraintName,
    )

    if tx1.RowsAffected > 0 {
        var sourceKeyColumnUsages []KeyColumnUsage

        tx2 := sourceDb.Table("KEY_COLUMN_USAGE").Order("`POSITION_IN_UNIQUE_CONSTRAINT` ASC").Find(
            &sourceKeyColumnUsages,
            "`CONSTRAINT_SCHEMA` = ? AND `CONSTRAINT_NAME` = ? AND `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
            sourceTable.TableSchema, sourceTableConstraint.ConstraintName, sourceTable.TableSchema, sourceTable.TableName,
        )

        if tx2.RowsAffected > 0 {
            sourceTableKeyColumns := make(map[string][]string)

            for _, sourceKeyColumnUsage := range sourceKeyColumnUsages {
                sourceTableKeyColumns[sourceReferentialConstraint.TableName] = append(sourceTableKeyColumns[sourceReferentialConstraint.TableName], fmt.Sprintf("`%s`", sourceKeyColumnUsage.ColumnName))
                sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName] = append(sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName], fmt.Sprintf("`%s`", sourceKeyColumnUsage.ReferencedColumnName))
            }

            return fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s) ON DELETE %s ON UPDATE %s",
                sourceReferentialConstraint.ConstraintName,
                strings.Join(sourceTableKeyColumns[sourceReferentialConstraint.TableName], ","),
                sourceReferentialConstraint.ReferencedTableName,
                strings.Join(sourceTableKeyColumns[sourceReferentialConstraint.ReferencedTableName], ","),
                sourceReferentialConstraint.DeleteRule, sourceReferentialConstraint.UpdateRule,
            )
        }
    }

    return ""
}

func getCharacterSet(sourceColumn Column, targetColumn Column) string {
    if sourceColumn.CharacterSetName.Valid && sourceColumn.CollationName.Valid {
        condition := false

        if sourceColumn.CharacterSetName.String != targetColumn.CharacterSetName.String {
            condition = true
        }

        if sourceColumn.CollationName.String != targetColumn.CollationName.String {
            condition = true
        }

        if sourceColumn == targetColumn {
            condition = true
        }

        if condition {
            return fmt.Sprintf(" CHARACTER SET %s COLLATE %s", sourceColumn.CharacterSetName.String, sourceColumn.CollationName.String)
        }
    }

    return ""
}

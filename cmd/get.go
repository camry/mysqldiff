package cmd

import (
    "fmt"
    "sort"
    "strings"
)

func getColumnNullAbleDefault(column Column) string {
    var nullAbleDefault = ""

    if column.IsNullable == "NO" {
        if column.ColumnDefault.Valid {
            if exists, _ := InArray(column.DataType, []string{"timestamp", "datetime"}); exists {
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
            if exists, _ := InArray(column.DataType, []string{"timestamp", "datetime"}); exists {
                if column.ColumnDefault.String != "CURRENT_TIMESTAMP" {
                    column.ColumnDefault.String = fmt.Sprintf("'%s'", column.ColumnDefault.String)
                }

                nullAbleDefault = fmt.Sprintf(" NULL DEFAULT %s", column.ColumnDefault.String)
            } else {
                nullAbleDefault = fmt.Sprintf(" DEFAULT '%s'", column.ColumnDefault.String)
            }
        } else {
            if exists, _ := InArray(column.DataType, []string{"timestamp", "datetime"}); exists {
                nullAbleDefault = " NULL DEFAULT NULL"
            } else {
                nullAbleDefault = " DEFAULT NULL"
            }
        }
    }

    return nullAbleDefault
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

func getColumnAfter(ordinalPosition int, columnsPos map[int]Column) string {
    pos := ordinalPosition - 1

    if _, ok := columnsPos[pos]; ok {
        return fmt.Sprintf("AFTER `%s`", columnsPos[pos].ColumnName)
    } else {
        return "FIRST"
    }
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

package cmd

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
